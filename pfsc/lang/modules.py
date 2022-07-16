# --------------------------------------------------------------------------- #
#   Proofscape Server                                                         #
#                                                                             #
#   Copyright (c) 2011-2022 Alpine Mathematics contributors                   #
#                                                                             #
#   Licensed under the Apache License, Version 2.0 (the "License");           #
#   you may not use this file except in compliance with the License.          #
#   You may obtain a copy of the License at                                   #
#                                                                             #
#       http://www.apache.org/licenses/LICENSE-2.0                            #
#                                                                             #
#   Unless required by applicable law or agreed to in writing, software       #
#   distributed under the License is distributed on an "AS IS" BASIS,         #
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  #
#   See the License for the specific language governing permissions and       #
#   limitations under the License.                                            #
# --------------------------------------------------------------------------- #

"""
Parse .pfsc modules, and build internal representations of the objects
they declare, using the classes in the deductions and annotations (python) modules.
"""

import datetime, re

from lark import Lark
from lark.exceptions import VisitError, LarkError

from pfsc.lang.annotations import Annotation
from pfsc.lang.freestrings import PfscJsonTransformer, json_grammar, json_grammar_imports
from pfsc.lang.deductions import PfscObj, Deduction, SubDeduc, Node, Supp, Flse, SPECIAL_NODE_CLASS_LOOKUP
from pfsc.lang.objects import PfscDefn
from pfsc.excep import PfscExcep, PECode
from pfsc.build.lib.libpath import PathInfo, get_modpath
from pfsc.build.repo import get_repo_part
from pfsc.build.versions import version_string_is_valid
from pfsc.gdb import get_graph_reader
from pfsc.permissions import have_repo_permission, ActionType
from pfsc_util.scan import PfscModuleStringAwareScanner
import pfsc.util as util
import pfsc.constants


class PfscModule(PfscObj):
    """
    Represents a whole Proofscape Module.
    """

    def __init__(self, libpath, loading_version=pfsc.constants.WIP_TAG, given_dependencies=None):
        """
        :param libpath: the libpath of this module.
        :param loading_version: the version of this module that we are using
          at the time that we are loading it.
        :param given_dependencies: a dependencies lookup which, if given, will
          override that which we obtain from our root module.
        """
        PfscObj.__init__(self)
        self.libpath = libpath
        self.repopath = get_repo_part(libpath)
        self.loading_version = loading_version
        # This module may represent a different version than that named by
        # the loading version. This is because when we are building, all modules
        # from the repo being built are always loaded at WIP version (with a possible
        # checkout of a tagged release prior to this); however, if it is a release
        # build, then they represent not WIP but their release number.
        # We initialize the represented version to the same value, but it can be
        # changed later.
        self.represented_version = loading_version
        self._modtext = None  # place to stash the original text of the module
        self._bc = None  # place to stash the BlockChunker that processed this module's text
        self.dependencies = None
        if given_dependencies is not None:
            self.load_and_validate_dependency_info()
            assert isinstance(self.dependencies, dict)
            self.dependencies.update(given_dependencies)

    def isRepo(self):
        return self.libpath == self.repopath

    def isSpecial(self):
        return self.libpath.startswith('special.')

    def setRepresentedVersion(self, vers):
        self.represented_version = vers

    def getDependencies(self):
        return self.dependencies or {}

    def load_and_validate_dependency_info(self):
        if self.dependencies is None:
            deps = {}
            if not self.isSpecial():
                root_module = self if self.isRepo() else load_module(
                    self.repopath, version=self.loading_version, fail_gracefully=True, history=[]
                )
                if root_module:
                    deps = root_module.getAsgnValue(pfsc.constants.DEPENDENCIES_LHS, default={})

            checked_deps = {}
            for r, v in deps.items():
                # Being drawn from a parsed module, the keys and values in the `deps` dict will be
                # not pure strings but `Markup` instances. We convert them into pure strings now,
                # to avoid potential trouble. In particular, the version will wind up being a parameter
                # in Cypher queries. In RedisGraph this used to lead to trouble when `Graph.build_params_header()`
                # (in redisgraph-py v2.4.0) added quotation marks to param values. The problem then was that, e.g.
                # `Markup('foo') + '"' --> Markup('foo&#34;')` and we got unwanted HTML escapes.
                # This has since been repaired, but I'm keeping the string conversion here just
                # to be on the safe side.
                r, v = str(r), str(v)
                if not version_string_is_valid(v, allow_WIP=True):
                    msg = f'Repo `{self.repopath}` defines invalid version `{v}` for dependency `{r}`.'
                    raise PfscExcep(msg, PECode.MALFORMED_VERSION_TAG)
                checked_deps[r] = v

            self.dependencies = checked_deps

    def getRequiredVersionOfObject(self, libpath, extra_err_msg='', loading_time=True):
        """
        Determine, according to the dependencies declaration for the repo to which
        this module belongs, what is the required version of a given object.

        :param libpath: the libpath of the object whose required version we want.
        :param extra_err_msg: extra string to be added onto the error message, in
            case the version number is unavailable. This can provide helpful info
            about where/why the version number is needed.
        :param loading_time: Is this request happening during module loading? This
            controls whether we use our loading version or represented version, when
            the object in question belongs to the same repo as us.
        :return: the required version string.
        """
        self.load_and_validate_dependency_info()
        repopath = get_repo_part(libpath)
        if repopath == self.repopath:
            return self.loading_version if loading_time else self.represented_version
        if repopath not in self.dependencies:
            msg = f'Repo `{self.repopath}` failed to define required version of `{repopath}`.'
            msg += extra_err_msg
            raise PfscExcep(msg, PECode.MISSING_REPO_DEPENDENCY_INFO)
        return self.dependencies[repopath]

    def getVersion(self):
        return self.represented_version

    def setBlockChunker(self, bc):
        self._bc = bc

    def getBlockChunker(self):
        return self._bc

    def modtext(self, *args):
        if len(args) == 0:
            return self._modtext
        else:
            self._modtext = args[0]

    def getBuildDirAndFilename(self, version=pfsc.constants.WIP_TAG):
        pi = PathInfo(self.libpath)
        return pi.get_build_dir_and_filename(version=version)

    def getBuiltVersion(self):
        return self._bc.write_module_text()

    def writeBuiltVersionToDisk(self, writeOnlyIfDifferent=True, makeTildeBackup=True):
        """
        Since the build process can supply missing widget names, it can be useful
        to write the built version to disk.

        :param writeOnlyIfDifferent: True means we will write to disk only if the text has changed.
        :param makeTildeBackup: True means that before writing the current version to disk, we rename
                                the existing version with a tilde at the end of the filename.
        :return: number of bytes written
        """
        text = self.getBuiltVersion()
        n = 0
        if (not writeOnlyIfDifferent) or text != self._modtext:
            pi = PathInfo(self.libpath)
            if makeTildeBackup:
                pi.make_tilde_backup()
            n = pi.write_module(text)
        return n

    def lazyLoadSubmodule(self, name):
        """
        Attempt to load a submodule.
        If we succeed, we both store the submodule in this module under its name,
        and return the submodule.

        @param name: the name of the supposed submodule
        @return: the loaded submodule, if found, else None
        """
        possible_submodule_path = '.'.join([self.libpath, name])
        sub = load_module(possible_submodule_path, version=self.loading_version, fail_gracefully=True)
        if sub is not None:
            # We did manage to load a submodule. Save it under its name.
            self[name] = sub
        return sub

    def isModule(self):
        return True

    def getNativeItemsInDefOrder(self, hoist_expansions=False):
        """
        Get an ordered dict of all native items in definition order.

        :param hoist_expansions: if True, we start with definition order, but then
          "hoist expansions," so that, if E --> D then E comes immediately after D.
        :return: ordered dict of native items
        """
        native = PfscObj.getNativeItemsInDefOrder(self)

        if hoist_expansions:
            nodes = {}  # lookup of DoublyLinkedListNodes by libpath of item
            start_node = util.DoublyLinkedListNode(None)
            prev = start_node
            for name, item in native.items():
                node = util.DoublyLinkedListNode((name, item), prev=prev)
                nodes[item.getLibpath()] = node
                if prev is not None:
                    prev.next = node
                prev = node
            for node in nodes.values():
                name, item = node.data
                if isinstance(item, Deduction):
                    tdlp = item.getTargetDeducLibpath()
                    if tdlp in nodes:
                        parent = nodes[tdlp]
                        node.extract()
                        parent.set_next(node)
            native = {}
            node = start_node.next
            seen = set()
            while node is not None:
                name, item = node.data
                native[name] = item
                node = node.next
                # There shouldn't be any cycles, but just in case...
                libpath = item.getLibpath()
                if libpath in seen:
                    raise PfscExcep(f'Cycle detected among expansions in module {self.libpath}', PECode.DAG_HAS_CYCLE)
                seen.add(libpath)

        return native

    def listNativeNodesByLibpath(self):
        nodelist = []
        deducs = self.getAllNativeDeductions()
        for deduc in deducs:
            deduc.listNativeNodesByLibpath(nodelist)
        return nodelist

    def getAllNativeNodes(self):
        nodelist = []
        deducs = self.getAllNativeDeductions()
        for deduc in deducs:
            deduc.getAllNativeNodes(nodelist)
        return nodelist

    def getAllNativeDeductions(self, toposort=True, numberednames=True):
        names = self.listNativeDeducsByName(toposort=toposort, numberednames=numberednames)
        deducs = [self[name] for name in names]
        return deducs

    def getAllDeductionsOfDeduction(self, T):
        """
        T an instance of the Deduction class, belonging to
        this module.
        Return a list of all Deductions declared in this module
        having T as target.
        """
        deds = []
        for item in self.items.values():
            if callable(getattr(item, 'getTargetDeduction', None)):
                S = item.getTargetDeduction()
                if S == T:
                    deds.append(item)
        return deds

    def buildAllDashgraphs(
            self,
            lang='en',
            nativeOnly=False,
            skiplist=None
    ):
        if skiplist is None: skiplist=[]
        dashgraphs = {}
        for item in self.items.values():
            if (
                callable(getattr(item, 'buildDashgraph', None)) and
                callable(getattr(item, 'getName', None))
            ):
                name = item.getName()
                if name in skiplist:
                    continue
                if nativeOnly:
                    # Build dg only for deducs native to this module.
                    ilp = item.getLibpath()
                    slp = self.libpath
                    m = len(ilp)
                    n = len(slp)
                    if ilp[:n] != slp or m <= n or ilp[n] != '.':
                        continue
                dg = item.buildDashgraph(lang=lang)
                dashgraphs[name] = dg
        return dashgraphs

    def listNativeDeducsByName(self, toposort=True, numberednames=True):
        """
        Return a list of the local names (e.g. "Thm", "Pf", "Xpan") of the
        deductions defined in this module.
        :param toposort: If True (the default) then the deduction names are
                         sorted in topological order, so that if E --> D then
                         D comes before E in the list.

                         This is useful e.g. for adding all the deducs to the
                         index, where D should have a row before we try to say
                         that E expands on D.
        :param numberednames: If True then, secondary to topological ordering,
                              names are sorted according to numerical suffixes.
                              E.g. this is so `Thm9` comes before `Thm10`.
                              See `NumberedName` class in util module.
        """
        # First find the names of all the native deductions.
        names = []
        slp = self.libpath
        n = len(slp)
        for name, item in self.items.items():
            # Is item a Deduction?
            if not isinstance(item, Deduction): continue
            # Is item native?
            ilp = item.getLibpath()
            m = len(ilp)
            if ilp[:n] != slp or m <= n or ilp[n] != '.':
                continue
            # Add name.
            names.append(name)
        # Now do topological sort, if requested.
        if toposort:
            # First must build digraph representation.
            graph = {}
            for E_name in names:
                E = self[E_name]
                D = E.getTargetDeduction()
                outnbrs = []
                if D is not None:
                    # D only belongs in the digraph if it is also native.
                    ilp = D.getLibpath()
                    m = len(ilp)
                    if ilp[:n] == slp and m > n and ilp[n] == '.':
                        D_name = D.getName()
                        outnbrs.append(D_name)
                graph[E_name] = outnbrs
            # Want e.g. Thm to come before Pf, so set reversed=True in topo sort.
            secondary_key = util.NumberedName if numberednames else None
            names = util.topological_sort(graph, reversed=True, secondary_key=secondary_key)
        return names


# TODO: "take REPO_PATH at COMMIT_HASH" ???
# Do we want to support a
#
#     take REPO_PATH at COMMIT_HASH
#
# statement? It would be kind of like explicitly noting, in any software project, the
# version number you require for any of your dependencies.

pfsc_grammar = r'''
    module : (import|deduc|anno|defn|tla)*

    ?import : plainimport
            | fromimport
    plainimport : "import" (relpath|libpath) ("as" IDENTIFIER)?
    fromimport : "from" (relpath|libpath) "import" (STAR|identlist ("as" IDENTIFIER)?)

    relpath : RELPREFIX libpath?
    libpath : IDENTIFIER ("." IDENTIFIER)*
    identlist : IDENTIFIER ("," IDENTIFIER)*

    deduc : deducpreamble "{" deduccontents "}"
    deducpreamble : "deduc" IDENTIFIER (OF targets)? (WITH targets)?
    targets : libpath ("," libpath)*

    deduccontents : (subdeduc|node|assignment)*

    subdeduc : "subdeduc" IDENTIFIER "{" deduccontents "}"

    ?node : basicnode
          | suppnode
          | wolognode
          | flsenode

    basicnode : NODETYPE IDENTIFIER "{" nodecontents "}"

    suppnode : "supp" IDENTIFIER ("versus" targets)? "{" nodecontents "}"

    wolognode : WOLOGTYPE IDENTIFIER "wolog" "{" nodecontents "}"

    flsenode : "flse" IDENTIFIER ("contra" targets)? "{" nodecontents "}"

    nodecontents : (node|assignment)*

    anno: "anno" IDENTIFIER ("on" targets)?

    defn: "defn" IDENTIFIER ve_string ve_string

    tla : assignment

    assignment : IDENTIFIER "=" json_value

    STAR : "*"

    OF: "of"

    WITH: "with"

    IDENTIFIER : CNAME

    RELPREFIX : "."+

    NODETYPE : "asrt"|"cite"|"exis"|"intr"|"mthd"|"rels"|"univ"|"with"

    WOLOGTYPE : "mthd"|"supp"
'''

# We don't actually need any further imports beyond the json_grammar_imports.
pfsc_grammar_imports = '''
'''

pfsc_parser = Lark(pfsc_grammar + json_grammar + pfsc_grammar_imports + json_grammar_imports, start='module')

BLOCK_RE = re.compile(r'(anno +([a-zA-Z]\w*)[^@]*?)@@@(\w{,8})(\s.*?)@@@\3', flags=re.S)

class BlockChunker:
    '''
    Contrary to the appearance of the pfsc_parser definition in EBNF above, "annotation blocks"
    in actual pfsc modules should look like this:

        anno <NameOfBlock> <targets>? """
            ...pfsc-flavored markdown...
        """

    However, it does not make sense to pass all of this directly to the Earley-algorithm parser
    we build using Lark.

    Furthermore, the markdown in the anno block is apt to contain many `#` chars (for headings),
    which we need to protect from the step that strips comments out of the module.

    Therefore we have this class to perform a pre-processing step in which we:
      (a) simplify the module before passing to Earley algorithm, and
      (b) protect `#` chars in anno blocks

    We locate all anno blocks, and take away the interior text and bracketing quotes, leaving
    just the `anno` keyword, the name of the block, and the targets (if any).

    We store the text so modified, along with dicts mapping the block names to the original
    text (minus bracketing quotes).
    '''
    def __init__(self, text):
        """
        :param text: the original text of a pfsc module
        """
        self.text = text
        chunks = BLOCK_RE.split(text)
        self.chunks = chunks
        self.n = int((len(chunks)-1)/5)

        # We will record data in order to achieve a "line mapping". This is so that line numbers in
        # the modified text we are going to produce can be mapped back to their correct values in the original text.
        # This is useful in reporting parsing errors or seeking a definition in a module.
        self.line_mapping = []
        self.line_no = 1
        self.lines_cut = 0
        def keep_lines(chunk):
            n = chunk.count('\n')
            self.line_no += n
        def cut_lines(chunk):
            n = chunk.count('\n')
            self.lines_cut += n
            self.line_mapping.append((self.line_no, self.lines_cut))

        # Record the first text chunk.
        c0 = chunks[0]
        mod_text_parts = [c0]
        keep_lines(c0)

        # anno_lookup maps the identifiers of anno blocks (their "names", in other words)
        # to the original text of those blocks (sans bracketing triple-quotes)
        self.anno_lookup = {}
        for k in range(self.n):
            # a : anno preamble
            # an: ANNONAME
            # ad: anno delimiter code
            # ac: ANNOCONTENTS
            # t : TEXT
            a, an, ad, ac, t = chunks[5*k+1:5*k+6]
            mod_text_parts.append(a)
            self.anno_lookup[an] = ac
            cut_lines(ac)
            mod_text_parts.append(t)
            keep_lines(t)

        # For use in computing line mappings, the list we recorded needs to be reversed.
        self.line_mapping.reverse()
        # Build the modified text.
        self.modified_text = ''.join(mod_text_parts)
        # annotations_by_name will map the names of Annotation instances to those instances;
        # these names are the same as the identifiers of the original anno blocks.
        self.annotations_by_name = {}

    def map_line_num_to_orig(self, n):
        for lb, a in self.line_mapping:
            if n > lb:
                n += a
                break
        return n

    def get_modified_text(self):
        return self.modified_text

    def add_annotation(self, a):
        """
        :param a: an Annotation instance
        """
        self.annotations_by_name[a.name] = a

    def write_module_text(self, widget_data=None):
        """
        Write the text for the module, after substituting new widget data.
        :param widget_data: dict mapping widget libpaths to new data for those widgets.
                            See the `Annotation` class's `write_anno_text` method for format.
        :return: the module text, with substitutions
        """
        if widget_data is None: widget_data = {}
        chunks = self.chunks
        mod_text_parts = [chunks[0]]
        for k in range(self.n):
            a, an, ad, ac, t = chunks[5*k+1:5*k+6]
            anno_obj = self.annotations_by_name[an]
            anno_text = anno_obj.write_anno_text(widget_data)
            anno_block = '%s@@@%s%s@@@%s' % (a, ad, anno_text, ad)
            mod_text_parts.append(anno_block)
            mod_text_parts.append(t)
        module_text = ''.join(mod_text_parts)
        return module_text


class CommentStripper(PfscModuleStringAwareScanner):

    def __init__(self):
        super().__init__()
        self.stripped_text = ''

    def state_0(self, c, i):
        next_state = None
        if c == "#":
            next_state = 'inside_comment'
        else:
            self.stripped_text += c
            if self.planned_next_state == 'd3':
                self.stripped_text += '""'
            elif self.planned_next_state == 's3':
                self.stripped_text += "''"
        return next_state, None

    def state_inside_comment(self, c, i):
        next_state = None
        if c == '\n':
            # Comment ends.
            # We keep the \n char in the output so that line numbering
            # is not altered by the comment stripping operation.
            self.stripped_text += c
            next_state = 0
        return next_state, None

    def state_d1(self, c, i):
        self.stripped_text += c
        return None, None

    state_s1 = state_d1

    def state_d3(self, c, i):
        self.stripped_text += c
        if self.planned_next_state == 0:
            self.stripped_text += '""'
        return None, None

    def state_s3(self, c, i):
        self.stripped_text += c
        if self.planned_next_state == 0:
            self.stripped_text += "''"
        return None, None


def strip_comments(text):
    """
    Strip all comments (full-line, or end-line) out of a given text.
    :param text: The text to be purged of comments.
    :return: The purged text.
    """
    cs = CommentStripper()
    cs.scan(text)
    return cs.stripped_text


def parse_module_text(text):
    """
    Parse a .pfsc module.
    :param text: The text of a .pfsc module.
    :return: (Lark Tree instance, BlockChunker instance)
    """
    # Simplify anno blocks.
    bc = BlockChunker(text)
    mtext = bc.get_modified_text()
    # Strip out all comments.
    mmtext = strip_comments(mtext)
    # Now parse, and return.
    try:
        tree = pfsc_parser.parse(mmtext)
    except LarkError as e:
        # Parsing error.
        # Restore original line numbers in error message.
        parse_msg = re.sub(r'at line (\d+)', lambda m: ('at line %s' % bc.map_line_num_to_orig(int(m.group(1)))), str(e))
        raise PfscExcep(parse_msg, PECode.PARSING_ERROR)
    return tree, bc

class PfscAssignment(PfscObj):

    def __init__(self, lhs, rhs, module=None):
        PfscObj.__init__(self)
        self.parent = module
        self.name = lhs
        self.lhs = lhs
        self.rhs = rhs

    def __str__(self):
        return "%s := %s" % (self.lhs, self.rhs)

    def get_rhs(self):
        return self.rhs

class PfscDeducPreamble:

    def __init__(self, name, targets, rdefs):
        self.name = name
        self.targets = targets
        self.rdefs = rdefs

class PfscRelpath:

    def __init__(self, num_dots, libpath):
        self.num_dots = num_dots
        self.libpath = libpath

    def __str__(self):
        return '.'*self.num_dots + self.libpath

    def resolve(self, home_path):
        """
        Resolve this relative path to an absolute one, given the absolute path
        of the module in which this relative one was invoked.
        @param home_path: the aboslute libpath of the module in which this was invoked
        @return: the absolute libpath to which this relative one resolves
        """
        # First dot means "this module", second dot means "the module above this one", and so on.
        # So the number of segments to be chopped is one less than the number of leading dots.
        chop_count = self.num_dots - 1
        assert chop_count >= 0
        home_parts = home_path.split('.')
        if chop_count > len(home_parts):
            msg = 'Malformed relative path: %s attempts to go above top of hierarchy.' % self
            raise PfscExcep(msg, PECode.MALFORMED_LIBPATH)
        # Subtlety: unfortunately slicing up to "negative 0" doesn't get you the whole list, so...
        keep_parts = home_parts[:-chop_count] if chop_count > 0 else home_parts[:]
        # The extension, i.e. the part that comes after the leading dots, may be empty.
        # But if it's not...
        if self.libpath:
            # ...then append it.
            keep_parts.append(self.libpath)
        # Now can join, to make the full absolute libpath.
        abspath = '.'.join(keep_parts)
        return abspath

class CachePolicy:
    """
    Enum class for ways of using the module cache.

    NEVER: Do not use the cache. Reload the module.
    ALWAYS: If the module is in the cache, use it. Don't worry about timestamps.
    TIME: Base use of the cache on timestamps. Let t_m be the modification time of the module on disk.
          Let t_r be the "read time" for the cached module (i.e. the time that it was read off disk).
          If t_m is later than t_r (i.e. the module has been modified since the last time it was
          read), then reload the module from disk; else, use the cache.
    """
    NEVER=0
    ALWAYS=1
    TIME=2

class ModuleLoader(PfscJsonTransformer):

    def __init__(self, modpath, bc, version=pfsc.constants.WIP_TAG, history=None, caching=CachePolicy.TIME, dependencies=None, do_imports=True):
        """
        :param modpath: The libpath of this module
        :param bc: The BlockChunker that performed the first chunking pass on this module's text
        :param version: The version being loaded
        :param history: a list of libpaths we have already imported; helps detect cyclic import errors
        :param caching: set the cache policy.
        :param dependencies: optionally pass a dict mapping repopaths to required versions. If provided,
          these will override what we would have determined by checking dependencies declared in the repo root module.
        :param do_imports: Set False if you don't actually want to perform imports. May be useful for testing.
        """
        # Since we are subclassing Transformer, and since our grammar has a nonterminal
        # named `module` (namely, the start symbol for our grammar), we use `self._module`
        # to store the PfscModule object we are building.
        self.modpath = modpath
        self.version = version
        self._module = PfscModule(modpath, loading_version=version, given_dependencies=dependencies)
        super().__init__(scope=self._module)
        self.bc = bc
        self._module.setBlockChunker(bc)
        self.do_imports = do_imports
        self.caching = caching
        self.history = history

    def get_desired_version_for_target(self, targetpath):
        """
        :param targetpath: The libpath of the target object.
        :return: the version at which we wish to take the repo to which the
          named target belongs.
        """
        extra_msg = f' Required for import in module `{self.modpath}`.'
        return self._module.getRequiredVersionOfObject(targetpath, extra_err_msg=extra_msg)

    def fromimport(self, items):
        """
        fromimport : "from" (relpath|libpath) "import" (STAR|identlist ("as" IDENTIFIER)?)
        """
        if self.do_imports:

            # Gather data.
            is_relative = isinstance(items[0], PfscRelpath)
            if is_relative:
                modpath = items[0].resolve(self.modpath)
            else:
                modpath = items[0]
            object_names = items[1]
            requested_local_name = items[2] if len(items) == 3 else None
            version = self.get_desired_version_for_target(modpath)

            # Now we have the module libpath and version from which we are attempting to import something.
            # What we're allowed to do depends on whether the modpath we've constructed is the
            # same as that of this module, or different.
            # Usually it will be different, and then we have these permissions:
            may_import_all = True
            may_search_within_module = True

            if modpath == self.modpath:
                # The modpaths will be the same in the special case where we are attempting to
                # import a submodule via a relative path.
                # For example, if the module a.b.c.d features the import statement
                #     from . import e
                # where a.b.c.d.e is a submodule of a.b.c.d, then this case arises.
                # In this case, submodules are the _only_ thing we can try to import.
                may_import_all = False
                may_search_within_module = False
                src_module = None
            else:
                # In all other cases, the modpath points to something other than this module.
                # We can now attempt to build it, in case it points to a module (receiving None if it does not).
                src_module = load_module(modpath, version=version, fail_gracefully=True, history=self.history, caching=self.caching)

            # Next behavior depends on whether we wanted to import "all", or named individual object(s).
            if object_names == "*":
                if not may_import_all:
                    # Currently the only time when importing all is prohibited is when we are
                    # trying to import from self.
                    msg = 'Module %s is attempting to import * from itself.' % self.modpath
                    raise PfscExcep(msg, PECode.CYCLIC_IMPORT_ERROR)
                # Importing "all".
                # In this case, the modpath must point to an actual module, or else it is an error.
                if src_module is None:
                    msg = 'Attempting to import * from non-existent module: %s' % modpath
                    raise PfscExcep(msg, PECode.MODULE_DOES_NOT_EXIST)
                all_names = src_module.listAllItems()
                for name in all_names:
                    self._module[name] = src_module[name]
            else:
                # Importing individual name(s).
                N = len(object_names)
                for i, object_name in enumerate(object_names):
                    # Set up the local name.
                    local_name = requested_local_name if requested_local_name and i == N-1 else object_name
                    # Initialize imported object to None, so we can check for success.
                    obj = None
                    # Construct the full path to the object, and compute the longest initial segment
                    # of it that points to a module.
                    full_object_path = modpath + '.' + object_name
                    object_modpath = get_modpath(full_object_path, version=version)
                    # If the object_modpath equals the libpath of the module we're in, this is a cyclic import error.
                    if object_modpath == self.modpath:
                        if full_object_path == self.modpath:
                            msg = f'Module {self.modpath} attempts to import itself from its ancestor.'
                        else:
                            msg = f'Module {self.modpath} attempts to import from within itself.'
                        raise PfscExcep(msg, PECode.CYCLIC_IMPORT_ERROR)
                    # The first attempt is to find the name in the source module, if any.
                    if src_module and may_search_within_module:
                        obj = src_module.get(object_name)
                    # If that failed, try to import a submodule.
                    if obj is None:
                        obj = load_module(full_object_path, version=version, fail_gracefully=True, history=self.history, caching=self.caching)
                    # If that failed too, it's an error.
                    if obj is None:
                        msg = 'Could not import %s from %s' % (object_name, modpath)
                        raise PfscExcep(msg, PECode.MODULE_DOES_NOT_CONTAIN_OBJECT)
                    # Otherwise, record the object.
                    else:
                        self._module[local_name] = obj

    def plainimport(self, items):
        if self.do_imports:
            # Are we using a relative libpath or absolute one?
            is_relative = isinstance(items[0], PfscRelpath)
            if is_relative:
                modpath = items[0].resolve(self.modpath)
                # In this case the user _must_ provide an "as" clause.
                if len(items) < 2:
                    msg = 'Plain import with relative libpath failed to provide "as" clause.'
                    raise PfscExcep(msg, PECode.PLAIN_RELATIVE_IMPORT_MISSING_LOCAL_NAME)
                local_path = items[1]
            else:
                modpath = items[0]
                # In this case an "as" clause is optional.
                local_path = items[1] if len(items) == 2 else modpath
            # Try to get the module, and store a reference to it in our module.
            v = self.get_desired_version_for_target(modpath)
            module = load_module(modpath, version=v, fail_gracefully=False, history=self.history, caching=self.caching)
            self._module[local_path] = module

    @staticmethod
    def ban_duplicates(names_encountered, next_name):
        """
        Raise an appropriate exception in case of duplicate definition.
        Otherwise, add name to set.

        :param names_encountered: Set of names encountered so far, within a
                                  context, such as a Node or Deduc defn.
        :param next_name: The next name to be considered. Should be a Lark Token,
                          from the original parsing process. This is because we want
                          to know the position where it occurred.
        :return: nothing
        :raises: PfscExcep

        :side-effect: the name will be added to the set, unless an exception is raised.
        """
        if next_name in names_encountered:
            msg = "Duplicate definition of %s at line %s, column %s" % (
                next_name, next_name.line, next_name.column
            )
            raise PfscExcep(msg, PECode.DUPLICATE_DEFINITION_IN_PFSC_MODULE)
        else:
            names_encountered.add(next_name)

    def set_first_line(self, obj, postchunk_lineno):
        """
        Tell an object the line number on which it begins.
        :param obj: the object whose line number is to be recorded. Must have a `setTextRange` method.
        :param postchunk_lineno: the line number where we "think" this object's definition begins,
            in the post-chunking universe. In other words, this is the line number that can be read
            off of the `line` attribute of a token occuring in the line in question. We will use our
            BlockChunker to restore this to the _actual_ line number in the original module text, and
            that is the number that will be recorded.
        :return: the _actual_ line number, as adjusted by the BlockChunker
        """
        actual_lineno = self.bc.map_line_num_to_orig(postchunk_lineno)
        obj.setTextRange(actual_lineno, None, None, None)
        return actual_lineno

    def set_contents(self, owner, contents, names=None):
        """
        Add contents to an owner (Deduc, Node, SubDeduc)

        :param owner: the owner to which the contents are to be added
        :param contents: the contents to be added to the owner
        :param names: the set of names already recorded in the owner
        """
        if names is None: names = set()
        for item in contents:
            # Must test if SubDeduc _before_ testing if Deduction, since the former is a subclass of the latter.
            if isinstance(item, SubDeduc):
                self.ban_duplicates(names, item.name)
                owner.addSubDeduc(item)
            elif isinstance(item, Node):
                self.ban_duplicates(names, item.name)
                owner.addNode(item)
            # Assignments are recorded as plain strings, under their given names.
            elif isinstance(item, PfscAssignment):
                self.ban_duplicates(names, item.lhs)
                owner[item.lhs] = item.rhs
            elif isinstance(item, (Deduction, Annotation, PfscDefn)):
                self.ban_duplicates(names, item.name)
                owner[item.name] = item

    def module(self, items):
        # Among the types of contents in a module are: imports, deducs, and other stuff.
        # The imports are not to be recorded; they just _do_ something.
        # The deducs already have been recorded. They record themselves, so that subsequent
        # deducs are able to find them. Likewise for annos and assignments.
        # Anything else is yet to be recorded in this module, so we do that here.
        existing_names = set(self._module.listAllItems())
        types_to_be_recorded = [
            # There used to be a couple of types needing to be recorded here,
            # but no more. For now we keep all this code in case it is useful
            # again at some point.
            #
            # To be clear, if one of the nonterminal handler methods for an
            # entity that can be defined at the top level of a pfsc module
            # returns items of class Foo, then you should list Foo here.
            #
            # For example, we used to list PfscAssignment here. But that ended
            # when we started recording these in the `assignment` nonterminal
            # handler method below.
            #
            # Generally speaking, we have switched to a design where top-level
            # entities record themselves in the module as soon as they can, so
            # that entities defined after them can reference them.
        ]
        items_to_be_recorded = []
        for item in items:
            for type_ in types_to_be_recorded:
                if isinstance(item, type_):
                    items_to_be_recorded.append(item)
                    break
        self.set_contents(self._module, items_to_be_recorded, existing_names)
        # Whether we keep the above, defunct code around or not, we still need to
        # return self._module.
        return self._module

    def anno(self, items):
        name = items[0]
        targets = items[1] if len(items) == 2 else []
        text = self.bc.anno_lookup[name]
        pa = Annotation(name, targets, text, self._module)
        # Add to module now. We do this here in this method so that objects
        # defined later in the same module can refer to this one. Furthermore,
        # we do it before the call to cascade libpaths so that relative libpaths
        # being resolved in widgets defined in this annotation can refer to this
        # annotation itself.
        self._module[name] = pa
        self.bc.add_annotation(pa)
        self.set_first_line(pa, name.line)
        # Ask the Annotation to build itself. This means it parses the pfsc-flavored markdown, builds widgets etc.
        pa.build()
        # Now that it has built its widgets, we can cascade and resolve libpaths.
        pa.cascadeLibpaths()
        pa.resolveLibpathsRec()
        return pa

    def deduc(self, items):
        preamble, contents = items
        ded = Deduction(preamble.name, preamble.targets, preamble.rdefs, module=self._module)
        self.set_first_line(ded, preamble.name.line)
        # Add to module now, so later defs can reference.
        self._module[ded.name] = ded
        self.set_contents(ded, contents)
        # Deductions are declared at the top level of modules,
        # so now we can let libpaths cascade down.
        ded.cascadeLibpaths()
        ded.resolve_objects()
        ded.buildGraph()
        return ded

    def subdeduc(self, items):
        name, contents = items
        subdeduc = SubDeduc(name)
        self.set_first_line(subdeduc, name.line)
        self.set_contents(subdeduc, contents)
        return subdeduc

    def defn(self, items):
        name, lhs, rhs = items
        defn = PfscDefn(name, lhs, rhs, module=self._module)
        # Add to module now, so later defs can reference.
        self._module[name] = defn
        defn.cascadeLibpaths()
        return defn

    def basicnode(self, items):
        type_, name, contents = items
        # Construct the node instance
        cls = SPECIAL_NODE_CLASS_LOOKUP.get(type_)
        if cls is None:
            node = Node(type_, name)
        else:
            node = cls(name)
        self.set_contents(node, contents)
        self.set_first_line(node, name.line)
        return node

    def suppnode(self, items):
        name = items[0]
        contents = items[-1]
        alternate_lps = items[1] if len(items) == 3 else []
        node = Supp(name)
        node.set_alternate_lps(alternate_lps)
        self.set_contents(node, contents)
        self.set_first_line(node, name.line)
        return node

    def wolognode(self, items):
        type_, name, contents = items
        cls = SPECIAL_NODE_CLASS_LOOKUP.get(type_)
        node = cls(name)
        node.set_wolog(True)
        self.set_contents(node, contents)
        self.set_first_line(node, name.line)
        return node

    def flsenode(self, items):
        name = items[0]
        contents = items[-1]
        contra = items[1] if len(items) == 3 else []
        node = Flse(name)
        node.set_contra(contra)
        self.set_contents(node, contents)
        self.set_first_line(node, name.line)
        return node

    def deducpreamble(self, items):
        name = items[0]
        targets = []
        rdefs = []
        n = len(items)
        for i in range(int((n-1)/2)):
            prep, paths = items[2*i+1:2*i+3]
            if prep == "of": targets = paths
            elif prep == "with": rdefs = paths
        return PfscDeducPreamble(name, targets, rdefs)

    def deduccontents(self, items):
        return items

    def nodecontents(self, items):
        return items

    def targets(self, items):
        return items

    def relpath(self, items):
        num_dots = len(items[0])
        libpath = items[1] if len(items) == 2 else ''
        return PfscRelpath(num_dots, libpath)

    def identlist(self, items):
        return items

    def libpath(self, items):
        return '.'.join(items)

    def assignment(self, items):
        lhs, rhs = items[:2]
        pa = PfscAssignment(lhs, rhs, module=self._module)
        return pa

    def tla(self, items):
        """
        "tla" = "top-level assignment". We use this syntactic distinction
        to let us know when we need to add an assignment to the module.
        """
        pa = items[0]
        self._module[pa.name] = pa
        pa.cascadeLibpaths()
        return pa

class CachedModule:

    def __init__(self, read_time, module):
        """
        @param read_time: a Unix timestamp for the time at which the module's contents were read off disk.
        @param module: the PfscModule to be cached
        """
        self.read_time = read_time
        self.module = module

class TimestampedText:

    def __init__(self, read_time, text):
        """
        @param read_time: a Unix timestamp for the time at which the text was read off disk.
        @param text: the text that was read
        """
        self.read_time = read_time
        self.text = text

_MODULE_CACHE = {}

def remove_modules_from_cache(modpaths, version=pfsc.constants.WIP_TAG):
    for modpath in modpaths:
        verspath = f'{modpath}@{version}'
        if verspath in _MODULE_CACHE:
            del _MODULE_CACHE[verspath]

def load_module(path_spec, version=pfsc.constants.WIP_TAG, text=None, fail_gracefully=False, history=None, caching=CachePolicy.TIME, cache=_MODULE_CACHE):
    """
    This is how you get a PfscModule object, i.e. an internal representation of a pfsc module.
    You can call it directly, or you can call it through a PathInfo object's own `load_module` method.
    If you do call it directly, you can pass a mere modpath string, or a PathInfo object built on such a string.

    @param path_spec: Either a string, being the libpath of the module you want to load, or else
                      a PathInfo object built on such a libpath. If you pass the string, we're
                      just going to start by constructing the PathInfo on it.

    @param version: The version of this module that you wish to load.

    @param text: If you happen to already have the text of the module, you can pass it here.
                 Note that when you provide the text we will definitely bypass the cache _for this module_;
                 meanwhile, cache use for any imports taking place _within_ the module will be controlled as
                 usual; see below.

                 The text can be passed either as a string, or as an instance of TimestampedText.
                 If the former, then we will _not_ cache the PfscModule object we construct on the given
                 text (because we cannot give it a timestamp). If the latter, we will cache it.

    @param fail_gracefully: What should happen if the libpath fails to point to a .pfsc file? If you just
                            want us to return `None`, then set this to `True`. Else we will raise an
                            appropriate `PfscException`.

    @param history: This is used for detecting cyclic import errors. Generally, you don't have to use this
                    yourself; it is already used appropriately by the system.

    @param caching: Here is where you can set the cache policy. See `CachePolicy` enum class.

    @param cache: The cache itself, where the modules are stored. Generally you don't need to use this yourself
                  (but you could, in order to have more fine-grained control over what gets reloaded and what
                  does not). Note that here we are using Python's #1 Gotcha -- the mutable default arg.
                  That means the dict we use as a cache is set at
                  function def time, and persists through all calls to this function (in other words, does
                  what you would want).

    @return: If successful, the loaded PfscModule instance is returned. If unsuccessful, then behavior depends
             on the `fail_gracefully` kwarg. If that is `True`, we will return `None`; otherwise, nothing will
             be returned, as a `PfscException` will be raised instead.
    """
    # Get a PathInfo object.
    if isinstance(path_spec, PathInfo):
        # You already gave us a PathInfo object.
        path_info = path_spec
    else:
        assert isinstance(path_spec, str)
        # You gave a libpath. Construct a PathInfo on it.
        path_info = PathInfo(path_spec)
    # Grab the modpath.
    modpath = path_info.libpath
    repopath = get_repo_part(modpath)
    if version == pfsc.constants.WIP_TAG and not have_repo_permission(
            ActionType.READ, repopath, pfsc.constants.WIP_TAG):
        msg = f'Insufficient permission to load `{modpath}` at WIP.'
        raise PfscExcep(msg, PECode.INADEQUATE_PERMISSIONS)
    # Function for the case where the libpath fails to point to a .pfsc file:
    def fail(force_excep=False):
        if fail_gracefully and not force_excep:
            return None
        else:
            msg = f'Could not find source code for module `{modpath}` at version `{version}`.'
            raise PfscExcep(msg, PECode.MODULE_HAS_NO_CONTENTS)
    # Now let's decide whether we're going to _reload_ the module (i.e. _not_ use the cache).
    # To begin with, if any of the following three conditions obtains, then yes we want to reload:
    #   (1) text given, (2) cache policy "never", or (3) cache miss.
    # So we begin by checking these.
    text_given = (text is not None)
    never_use_cache = (caching == CachePolicy.NEVER)
    verspath = f'{modpath}@{version}'
    cache_miss = (verspath not in cache)
    if text_given or never_use_cache or cache_miss:
        should_reload = True
    else:
        # In this case, the text was not given, and it's a cache hit, and the cache policy is
        # either "always" or the time-based policy.
        # If it's the "always" policy, then we do want to use the cache, i.e. do _not_ want to reload.
        if caching == CachePolicy.ALWAYS:
            should_reload = False
        # Or, if we want a numbered release version, then there's no question of it having
        # changed since last load. Numbered releases, by definition, do not change!
        elif version != pfsc.constants.WIP_TAG:
            should_reload = False
        else:
            # In this case, it should be the time-based cache policy.
            assert caching == CachePolicy.TIME
            # We need to compare the modification time to the read time.
            t_r = cache[verspath].read_time
            t_m = path_info.pfsc_file_modification_time
            if t_m is None: return fail()
            # We should reload the module if it has been modified since it was last read.
            # SUBTLETY: The Unix mtime timestamp is truncated to next lowest integer; the
            # read time is not. This can result in cases where, if we built a module and
            # then quickly modified and wrote it, the read time can appear to be after the
            # modification time. To prevent this, we give the modification time a "boost"
            # by adding one to it.
            should_reload = t_m + 1 >= t_r
    # Are we reloading?
    if should_reload:
        # Only when we have to reload does history become relevant, so we deal with it now.
        # If history is None, we actually want an empty list. As opposed to our deliberate use of a mutable
        # default arg for the `cache` kwarg, here we have to _work around_ that behavior.
        if history is None: history = []
        # If the module we're trying to build is already in the history of imports that have
        # led us to this point, then we raise a cyclic import error.
        if modpath in history:
            msg = 'Cyclic import error: \n    ' + (' -->\n    '.join(history + [modpath]))
            raise PfscExcep(msg, PECode.CYCLIC_IMPORT_ERROR)
        # Otherwise, add this modpath to the history.
        history.append(modpath)
        # We need an instance of TimestampedText.
        if text_given:
            # Text was given.
            if not isinstance(text, TimestampedText):
                # If text was provided but is not already an instance of TimestampedText, then it must be a string.
                assert isinstance(text, str)
                # We set `None` as timestamp, to ensure the module will not be cached.
                ts_text = TimestampedText(None, text)
            else:
                ts_text = text
        else:
            # Text was not given, so we need to read it from disk.
            if version != pfsc.constants.WIP_TAG:
                # If it's not a WIP module, we need the desired version to have
                # already been built and indexed. Check that now.
                if not get_graph_reader().version_is_already_indexed(repopath, version):
                    msg = f'Trying to load `{modpath}` at release `{version}`'
                    msg += ', but that version has not been built yet on this server.'
                    raise PfscExcep(msg, PECode.VERSION_NOT_BUILT_YET)
            # To be on the safe side, make timestamp just _before_ performing the read operation.
            # (This is "safe" in the sense that the read operation looks older, so we will be more
            # likely to reload in the future, i.e. to catch updates.)
            read_time = datetime.datetime.now().timestamp()
            try:
                module_contents = path_info.read_module(version=version)
            except FileNotFoundError:
                return fail(force_excep=(version!=pfsc.constants.WIP_TAG))
            ts_text = TimestampedText(read_time, module_contents)
        # Construct a PfscModule.
        module = build_module_from_text(ts_text.text, modpath, version=version, history=history, caching=caching)
        # If everything is working correctly, then now is the time to "forget" this
        # module, i.e. to erase its record from the history list; it should be the last record.
        to_forget = history.pop()
        assert to_forget == modpath
        # If the module text has a timestamp, then save the module in the cache.
        if ts_text.read_time is not None:
            cm = CachedModule(ts_text.read_time, module)
            cache[verspath] = cm
        # Finally, we can return the module.
        return module
    else:
        # We are not reloading, i.e. we are using the cache.
        module = cache[verspath].module
        return module

def build_module_from_text(text, modpath, version=pfsc.constants.WIP_TAG, history=None, caching=CachePolicy.TIME, dependencies=None):
    """
    Build a module, given its text, and its libpath.
    :param text: (str) the text of the module
    :param modpath: (str) the libpath of the module
    :param version: (str) the version being built
    :param history: (list) optional history of modules built; useful for
      detecting cyclic import errors
    :param caching: the desired CachePolicy
    :param dependencies: optionally pass a dict mapping repopaths to required versions.
    :return: the PfscModule instance constructed
    """
    tree, bc = parse_module_text(text)
    loader = ModuleLoader(modpath, bc, version=version, history=history, caching=caching, dependencies=dependencies)
    try:
        module = loader.transform(tree)
    except VisitError as v:
        # Lark traps our PfscExceps, re-raising them within a VisitError. We want to see the PfscExcep.
        raise v.orig_exc from v
    return module
