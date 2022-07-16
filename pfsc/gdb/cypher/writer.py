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

import pfsc.constants
from pfsc.constants import IndexType
from pfsc.gdb.writer import GraphWriter
import pfsc.gdb.cypher.indexing as indexing
from pfsc.excep import PfscExcep


class CypherGraphWriter(GraphWriter):

    def __init__(self, reader):
        super().__init__(reader)
        self.session = self.gdb.session()
        # TODO: Maybe the gdb teardown needs to close the session?

    def new_transaction(self):
        return self.session.begin_transaction()

    def commit_transaction(self, tx):
        tx.commit()

    def rollback_transaction(self, tx):
        tx.rollback()

    def _drop_wip_nodes_under_module(self, modpath, tx):
        tx.run(f"""
        MATCH (u {{modpath: $modpath, major: $WIP}})
        OPTIONAL MATCH (u)-[:{IndexType.BUILD}]->(b)
        DETACH DELETE u, b
        """, modpath=modpath, WIP=pfsc.constants.WIP_TAG)

    def _undo_wip_cut_nodes(self, node_db_ids, tx):
        tx.run("""
        MATCH (u {cut: $WIP}) WHERE id(u) IN $ids SET u.cut = $inf
        """, ids=node_db_ids, WIP=pfsc.constants.WIP_TAG, inf=pfsc.constants.INF_TAG)

    def _undo_wip_cut_relns(self, reln_db_ids, tx):
        tx.run("""
        MATCH ()-[r {cut: $WIP}]->() WHERE id(r) IN $ids SET r.cut = $inf
        """, ids=reln_db_ids, WIP=pfsc.constants.WIP_TAG, inf=pfsc.constants.INF_TAG)

    def ix0200(self, mii, tx):
        indexing.ix00220(mii, tx)
        indexing.ix00240(mii, tx)
        indexing.ix00261(mii, tx)
        new_targeting_relns = indexing.ix00282s(mii, tx)
        return new_targeting_relns

    def ix0330(self, mii, tx, verbose=False):
        items = mii.move_mapping.items()
        if verbose:
            print(f'Adding {len(items)} new moves.')
        move_counter, void_counter = 0, 0
        mii.note_begin_indexing_phase(330)
        for src, dst in items:
            if dst is None:
                void_counter += 1
                tx.run(
                    f"""
                        MATCH (s {{libpath: $src}}) WHERE s.major <= $cmv < s.cut
                        MERGE (d:{IndexType.VOID})
                        CREATE (s)-[:{IndexType.MOVE}]->(d)
                        """, src=src, cmv=mii.current_maj_vers
                )
            else:
                move_counter += 1
                tx.run(
                    f"""
                        MATCH (s {{libpath: $src}}), (d {{libpath: $dst, major: $major}})
                        WHERE s.major <= $cmv < s.cut
                        CREATE (s)-[:{IndexType.MOVE}]->(d)
                        """, src=src, dst=dst, major=mii.major,
                    cmv=mii.current_maj_vers
                )
            mii.note_task_element_completed(330)
        if verbose:
            print(f'  ({move_counter}) ?:{IndexType.MOVE}:?')
            print(f'  ({void_counter}) ?:{IndexType.MOVE}:{IndexType.VOID}')

    def ix0360(self, mii, tx, new_targeting_relns, verbose=False):
        if verbose:
            print('Searching for retargeting relations...')
        mii.note_begin_indexing_phase(360)
        retarget_counter = 0
        # (1) Enrichments we have added:
        for k in new_targeting_relns:
            mcs = self.reader.find_move_conjugate_chain(k.head_libpath, k.head_major)
            if mcs:
                retarget_counter += len(mcs)
                tx.run(
                    f"""
                    MATCH (e {{libpath: $tail_libpath}}) WHERE e.major <= $tail_major < e.cut
                    WITH e
                    UNWIND $mc_ids as mc_id
                    MATCH (mc) WHERE id(mc) = mc_id
                    CREATE (e)-[:{IndexType.RETARGETS}]->(mc)
                    """,
                    tail_libpath=k.tail_libpath, tail_major=k.tail_major,
                    mc_ids=[mc.db_uid for mc in mcs]
                )
            mii.note_task_element_completed(361)

        # (2) Existing enrichments on anything we moved:
        ids = [mii.existing_k_nodes[a].db_uid for a, b in
               mii.mm_closure.items() if b is not None]
        res = tx.run(
            f"""
            MATCH (e)-[:{IndexType.TARGETS}|{IndexType.RETARGETS}]->(t) WHERE id(t) IN $ids
            RETURN id(e), t.libpath
            """,
            ids=ids
        )
        pairs = [[eid, mii.mm_closure[tlp]] for eid, tlp in res]
        retarget_counter += len(pairs)
        tx.run(
            f"""
            UNWIND $pairs AS pair
            MATCH (e), (r {{libpath: pair[1]}}) WHERE id(e) = pair[0] AND r.major <= $major < r.cut
            CREATE (e)-[:{IndexType.RETARGETS}]->(r)
            """,
            pairs=pairs, major=mii.major
        )
        mii.note_task_element_completed(362, len(ids))
        if verbose:
            print(f'  ({retarget_counter}) ?:{IndexType.RETARGETS}:?')

    def ix0400(self, mii, tx):
        tx.run(
            f"""
                MERGE (v:{IndexType.VERSION} {{repopath: $repopath, version: $version}})
                SET v += $props
                """, repopath=mii.repopath, version=mii.version,
            props=mii.write_version_node_props()
        )

    def clear_test_indexing(self):
        self.session.run(f"""
        MATCH (u) WHERE u.repopath STARTS WITH 'test.'
        OPTIONAL MATCH (u)-[:{IndexType.BUILD}]->(b)
        DETACH DELETE u, b
        """)
        self.session.run("MATCH (u:User) WHERE u.username STARTS WITH 'test.' DETACH DELETE u")

    def _do_delete_all_under_repo(self, repopath):
        self.session.run(f"""
        MATCH (u {{repopath: $repopath}})
        OPTIONAL MATCH (u)-[:{IndexType.BUILD}]->(b)
        DETACH DELETE u, b
        """, repopath=repopath)

    def delete_full_wip_build(self, repopath):
        self.session.run(f"""
        MATCH (u {{repopath: $repopath}})
        WHERE u.major = $WIP OR u.version = $WIP
        OPTIONAL MATCH (u)-[:{IndexType.BUILD}]->(b)
        DETACH DELETE u, b
        """, repopath=repopath, WIP=pfsc.constants.WIP_TAG)

    # ----------------------------------------------------------------------

    def _add_user(self, username, j_props):
        self.session.run(f"""
        MERGE (u:{IndexType.USER} {{username: $username}})
        SET u.properties = $j_props
        """, username=username, j_props=j_props)

    def delete_user(self, username, *,
                    definitely_want_to_delete_this_user=False):
        if not definitely_want_to_delete_this_user:
            return 0
        res = self.session.run(f"""
        MATCH (u:{IndexType.USER} {{username: $username}})
        DETACH DELETE u
        """, username=username)
        info = res.consume()
        return info.counters.nodes_deleted

    def delete_all_notes_of_one_user(self, username, *,
                    definitely_want_to_delete_all_notes=False):
        if not definitely_want_to_delete_all_notes:
            return
        self.session.run(f"""
        MATCH (:{IndexType.USER} {{username: $username}})-[e:{IndexType.NOTES}]->()
        DELETE e
        """, username=username)

    def _update_user(self, username, j_props):
        self.session.run(f"""
        MATCH (u:{IndexType.USER} {{username: $username}})
        SET u.properties = $j_props
        """, username=username, j_props=j_props)

    def record_user_notes(self, username, user_notes):
        major0 = self.reader.adaptall(user_notes.goal_major)
        # It's important that we structure this as a transaction, for the case
        # of the user of the one-container app on their own machine. There we
        # use RedisGraph, and it's only a call to `commit_transaction()` that
        # prompts our `RedisGraphWrapper` class to dump to disk. The user's
        # notes should always be persisted to disk as soon as they're recorded
        # in the GDB. In other cases -- say, Neo4j in a production setting --
        # structuring as a transaction does no harm.
        tx = self.new_transaction()
        try:
            res = tx.run(f"""
            MATCH (g {{libpath: $goalpath, major: $major}}) RETURN id(g)
            """, goalpath=user_notes.goalpath, major=major0)
            rec = res.single()
            if rec is None:
                raise PfscExcep(f'Cannot record notes. Origin {user_notes.write_origin()} does not exist.')
            goal_db_id = rec.value()

            if user_notes.is_blank():
                tx.run(f"""
                MATCH (u:{IndexType.USER} {{username: $username}})-[e:{IndexType.NOTES}]->(g)
                WHERE ID(g) = $goal_db_id
                DELETE e
                """, username=username, goal_db_id=goal_db_id)
            else:
                tx.run(f"""
                MATCH (u:{IndexType.USER} {{username: $username}}), (g)
                WHERE ID(g) = $goal_db_id
                MERGE (u)-[e:{IndexType.NOTES}]->(g)
                SET e.state = $state
                SET e.notes = $notes
                """, username=username, goal_db_id=goal_db_id, state=user_notes.state, notes=user_notes.notes)
        except:
            self.rollback_transaction(tx)
            raise
        else:
            self.commit_transaction(tx)

    # ----------------------------------------------------------------------

    def record_module_source(self, modpath, version, modtext):
        major0 = self.reader.adaptall(version)
        self.session.run(f"""
        MATCH (m:{IndexType.MODULE} {{libpath: $modpath}})
        WHERE m.major <= $major < m.cut
        CREATE (m)-[b:{IndexType.BUILD}]->(s:{IndexType.MOD_SRC})
        SET b.{IndexType.P_BUILD_VERS} = $version
        SET s.pfsc = $modtext
        """, modpath=modpath, major=major0, version=version, modtext=modtext)

    def record_repo_manifest(self, repopath, version, manifest_json):
        self.session.run(f"""
        MATCH (v:{IndexType.VERSION} {{repopath: $repopath, version: $version}})
        SET v.manifest = $manifest_json
        """, repopath=repopath, version=version, manifest_json=manifest_json)

    def record_dashgraph(self, deducpath, version, dg_json):
        major0 = self.reader.adaptall(version)
        self.session.run(f"""
        MATCH (d:{IndexType.DEDUC} {{libpath: $deducpath}})
        WHERE d.major <= $major < d.cut
        CREATE (d)-[b:{IndexType.BUILD}]->(db:{IndexType.DEDUC_BUILD})
        SET b.{IndexType.P_BUILD_VERS} = $version
        SET db.json = $dg_json
        """, deducpath=deducpath, major=major0, version=version, dg_json=dg_json)

    def record_annobuild(self, annopath, version, anno_html, anno_json):
        major0 = self.reader.adaptall(version)
        self.session.run(f"""
        MATCH (a:{IndexType.ANNO} {{libpath: $annopath}})
        WHERE a.major <= $major < a.cut
        CREATE (a)-[b:{IndexType.BUILD}]->(ab:{IndexType.ANNO_BUILD})
        SET b.{IndexType.P_BUILD_VERS} = $version
        SET ab.html = $anno_html
        SET ab.json = $anno_json
        """, annopath=annopath, major=major0, version=version, anno_html=anno_html, anno_json=anno_json)

    def delete_builds_under_module(self, modpath, version):
        self.session.run(f"""
        MATCH (u {{modpath: $modpath}})-[b:{IndexType.BUILD}]->(v)
        WHERE b.{IndexType.P_BUILD_VERS} = $version
        DETACH DELETE v
        """, modpath=modpath, version=version)

    # ----------------------------------------------------------------------

    def _set_approvals_dict_json(self, widgetpath, version, j):
        major0 = self.reader.adaptall(version)
        self.session.run(f"""
        MATCH (w:{IndexType.WIDGET} {{libpath: $widgetpath}})
        WHERE w.major <= $major < w.cut
        SET w.{IndexType.P_APPROVALS} = $approvals
        """, widgetpath=widgetpath, major=major0, approvals=j)
