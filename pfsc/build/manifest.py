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
Tools for working with repo manifests.

A manifest is a record of all the contents of a repo, as well as the
timestamps and commit hashes of the most recent build.
"""

import json
from functools import lru_cache

import pfsc.constants
from pfsc.build.repo import get_repo_info
from pfsc.excep import PfscExcep, PECode
from pfsc.gdb import get_graph_reader, building_in_gdb


def build_manifest_tree_from_dict(d, parent=None):
    """
    Build _just a manifest tree model_ from a dictionary repr of one.

    WARNING: The given dict is destroyed in the process. So make a deep copy of it first, if
    you need it to remain intact. (Hint: use `json.loads(json.dumps(d))` for an easy deep copy operation.)

    :param d: The dictionary from which to build.
    :param parent: Ignore this; it's for managing recursion.
    :return: The Manifest whose tree represents the given dict.
    """
    id_ = d.pop("id")
    try:
        children = d.pop("children")
    except KeyError:
        children = []
    node = ManifestTreeNode(id_, **d)
    if parent is None:
        r = Manifest(node)
    else:
        r = None
        parent.add_child(node)
    for child in children:
        build_manifest_tree_from_dict(child, node)
    return r


def build_manifest_from_dict(d):
    """
    This function builds an entire manifest, with both tree_model _and_ build info,
    from a dictionary featuring same.

    Uses the `build_manifest_tree_from_dict` function, so the same warning applies, about the
    given dict being destroyed.

    :param d: The dictionary from which to build.
    :return: The Manifest.
    """
    manifest = build_manifest_tree_from_dict(d["tree_model"])
    manifest.set_build_info_dict(d["build"])
    return manifest


def has_manifest(repopath, version=pfsc.constants.WIP_TAG):
    if building_in_gdb():
        return get_graph_reader().has_manifest(repopath, version)
    else:
        ri = get_repo_info(repopath)
        return ri.has_manifest_json_file(version=version)


@lru_cache(maxsize=16)
def load_manifest_with_cache(libpath, control_code, version=pfsc.constants.WIP_TAG):
    """
    Convnience function to get a repo manifest from any libpath within (or equal to) it.
    :param libpath: the libpath of the repo, or of anything inside it
    :param control_code: for cache control
    :param version: the desired build version.
    :return: a built Manifest instance
    """
    ri = get_repo_info(libpath)
    try:
        if building_in_gdb():
            j = get_graph_reader().load_manifest(ri.libpath, version)
        else:
            path = ri.get_manifest_json_path(version=version)
            with open(path) as f:
                j = f.read()
    except FileNotFoundError:
        msg = f'Manifest not found for {libpath} at version {version}.'
        raise PfscExcep(msg, PECode.MISSING_MANIFEST)
    d = json.loads(j)
    manifest = build_manifest_from_dict(d)
    return manifest

def load_manifest(libpath, cache_control_code=None, version=pfsc.constants.WIP_TAG):
    if cache_control_code is None:
        return load_manifest_with_cache.__wrapped__(libpath, None, version=version)
    else:
        return load_manifest_with_cache(libpath, cache_control_code, version=version)

class Manifest:
    """
    Represents all the stuff in a container. Useful for recording and manipulating the data required
    for generating a tree view at front end.
    """

    def __init__(self, root_node):
        """
        :param root_node: a ManifestTreeNode to represent the root of the manifest
        """
        self.root_node = root_node
        # Lookup for nodes by their unique IDs:
        self.lookup = {}
        self.add_node(root_node)
        self.build_info = {}

    def get_build_info(self):
        return self.build_info

    def get_root_node(self):
        """Get the root node of the repo. """
        return self.root_node

    def is_single_build(self):
        """Say whether this manifest represents just a single build. """
        return len(list(self.build_info.keys())) == 1

    def get_single_build_node(self):
        """
        If this is a single-build manifest, get the node at which the build
        was performed.
        """
        libpath = list(self.build_info.keys())[0]
        return self.get(libpath)

    def write_tree_model_json(self):
        t = self.root_node.build_dict()
        return json.dumps(t)

    def set_build_info(self, libpath, version, commit, time, recursive):
        self.build_info = {
            libpath: {
                "version": version,
                "commit": commit,
                "time": str(time),
                "recursive": recursive
            }
        }

    def set_build_info_dict(self, d):
        self.build_info = d

    def build_dict(self):
        """
        :return: A dictionary representation of this object, suitable for writing as JSON.
        """
        d = {}
        if self.build_info:
            d["build"] = self.build_info
        t = self.root_node.build_dict()
        d["tree_model"] = t
        return d

    def get(self, key):
        return self.lookup.get(key)

    def add_node(self, node):
        # Tell the node that this is its manifest.
        node.manifest = self
        # And record the node in the global lookup, by its id.
        self.lookup[node.id] = node

    def merge(self, other):
        """
        Merge another manifest into this one.

        The other manifest must represent a single build operation. I.e. its
        build info should only contain a single entry.
        """
        if not isinstance(other, Manifest) or not other.is_single_build():
            raise PfscExcep(
                'Cannot merge. Other must be single-build manifest.', PECode.MANIFEST_BAD_FORM
            )

        sb, ob = self.build_info, other.build_info
        built_libpath, build_info = list(ob.items())[0]
        recursive = build_info['recursive']

        # If recursive, then remove any keys from sb of which k is a segmentwise prefix.
        if recursive:
            n = len(built_libpath)
            remove_keys = [
                k1 for k1 in sb if k1[:n] == built_libpath and k1[n:n+1] in ['', '.']
            ]
            for k1 in remove_keys:
                del sb[k1]

        sb[built_libpath] = build_info

        # Merge trees
        # Start by finding the first ancestor of B (including B itself) for
        # which we have a node A of the same id (libpath). There should be one,
        # since we're in the same repo, so we at least have the repo as a
        # common ancestor.
        A, B, C = None, other.get(built_libpath), None
        while B is not None and (A := self.get(B.id)) is None:
            C, B = B, B.parent
        if A is None or B is None:
            msg = 'Cannot merge repo manifests.'
            msg += ' You might need to try rebuilding the repo recursively from its root level.'
            raise PfscExcep(msg, PECode.MANIFEST_BAD_FORM)
        # If we have a node matching the module that was newly built, then we
        # want to replace our node with the new node.
        if A.id == built_libpath:
            if A is self.root_node:
                self.root_node = B
            else:
                A.parent.replace(A, B)
            # If the incoming build was not recursive, then we want to save
            # any pre-existing submodules.
            if not recursive:
                submodules = A.get_submodules()
                B.add_children(submodules)
        # But if our first matching node matched some proper ancestor of the
        # newly built module, then we can just add the new node as a child.
        else:
            A.add_child(C)

        # Update the lookup
        # Note: we're not bothering to remove from the lookup any old items that no longer exist.
        # For now at least, this does not cause any problems.
        self.lookup.update(other.lookup)


class ManifestTreeNode:
    """
    Represents a single node in a manifest tree.
    """

    def __init__(self, id_, **kwargs):
        self.manifest = None
        self.parent = None
        self.id = id_
        self.data = kwargs
        self.children = []
        self.data['libpath'] = id_

    def build_dict(self):
        """
        :return: A dictionary representation of this object, suitable for writing as JSON.
        """
        d = {"id": self.id}
        for k, v in self.data.items():
            d[k] = v
        children = []
        for child in self.children:
            c = child.build_dict()
            children.append(c)
        if children:
            d["children"] = children
        return d

    def build_relational_model(self, items, recursive=True, siblingOrder=0):
        """
        Build a list of items in the tree rooted at this node.

        It is a "relational model" in that items are _not_ nested. Instead, each item has a `parent`
        attribute, giving either the id of the parent item, or None if no parent.

        Each item also gets a "sibling" attribute, giving its order among its siblings.

        :param items: Pass the list you want to be populated with all the items.
        :param recursive: set False in order to skip submodules, and build items
                          only for "content" nodes. This would be appropriate for an update
                          after rebuilding a module non-recursively.
        :param siblingOrder: Value for the `sibling` attribute.
        :return: nothing. The `items` list you pass is modified in-place.
        """
        d = {"id": self.id, "sibling": siblingOrder}
        d["parent"] = self.parent.id if self.parent else None
        for k, v in self.data.items():
            d[k] = v
        items.append(d)
        am_module = self.is_module()
        if am_module:
            # A module is assumed terminal until proven otherwise.
            d["terminal"] = True
        for i, child in enumerate(self.children):
            if child.is_module():
                if am_module:
                    # We have at least one child that is a module, so we are not terminal.
                    d["terminal"] = False
                if not recursive:
                    continue
            child.build_relational_model(items, recursive=recursive, siblingOrder=i)

    def is_module(self):
        return self.data.get("type") == "MODULE"

    def get_submodules(self):
        """
        The submodules are all the children of type "MODULE".
        :return: list of all submodules
        """
        return filter(lambda c: c.data.get("type") == "MODULE", self.children)

    def get_contents(self):
        """
        The "contents" are all the children _not_ of type "MODULE".
        :return: list of all content items
        """
        return filter(lambda c: c.data.get("type") != "MODULE", self.children)

    def add_child(self, child):
        self.children.append(child)
        self.manifest.add_node(child)
        child.parent = self

    def add_children(self, children):
        for child in children:
            self.add_child(child)

    def replace(self, old, new):
        """
        Replace an existing child node with another node.
        """
        for i, c in enumerate(self.children):
            if c.id == old.id:
                self.children[i] = new
                new.parent = self
                break
        else:
            raise PfscExcep('Child %s not found' % old.id)