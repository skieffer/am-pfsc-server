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

import os
from datetime import datetime, timedelta, timezone

import pytest

from pfsc import check_config
from pfsc.build.repo import RepoInfo
from pfsc.build.demo import (
    make_demo_repo,
    delete_demo_repo,
    schedule_demo_repo_for_deletion,
    check_demo_repo_deletion_time,
    cancel_scheduled_demo_repo_deletion,
)
from pfsc.util import count_pfsc_modules

# Is there a way to reference fixtures (like `app`) in a `skipif`?
#@pytest.mark.skipif(not app.config.get("PFSC_DEMO_ROOT"))
def test_make_and_delete_demo_repo(app):
    if not app.config.get("PFSC_DEMO_ROOT"):
        return
    with app.app_context():
        repopath = 'demo.q123.workbook'
        ri = RepoInfo(repopath)
        # Start with a deletion to make sure starting from a clean state.
        delete_demo_repo(repopath)
        assert not os.path.exists(ri.abs_fs_path_to_dir)
        make_demo_repo(ri)
        assert count_pfsc_modules(ri.abs_fs_path_to_dir) > 0
        delete_demo_repo(repopath)
        assert not os.path.exists(ri.abs_fs_path_to_dir)

def test_schedule_demo_repo_deletion(app):
    if not app.config.get("PFSC_DEMO_ROOT"):
        return
    with app.app_context():
        repopath = 'demo.q123.workbook'
        ri = RepoInfo(repopath)
        # Start with clean state.
        delete_demo_repo(repopath)
        assert not os.path.exists(ri.abs_fs_path_to_dir)
        # Make a demo repo.
        make_demo_repo(ri)
        assert count_pfsc_modules(ri.abs_fs_path_to_dir) > 0
        # Schedule it for deletion.
        schedule_demo_repo_for_deletion(repopath)
        del_time = check_demo_repo_deletion_time(repopath)
        print(del_time)
        # The scheduled time should be based on the config.
        # We do our own computation here...
        now = datetime.utcnow()
        now = now.replace(tzinfo=timezone.utc)
        delta = timedelta(hours=check_config("DEMO_REPO_HOURS_TO_LIVE"))
        later = now + delta
        # ...and compare:
        dt = (later - del_time).total_seconds()
        print(dt)
        assert abs(dt) < 10
        # Clean up
        cancel_scheduled_demo_repo_deletion(repopath)
        assert check_demo_repo_deletion_time(repopath) is None
        delete_demo_repo(repopath)
        assert not os.path.exists(ri.abs_fs_path_to_dir)

@pytest.mark.parametrize('user, repo', [
    ['D935MN8', 'workbook'],
])
def test_delete_demo_repo(app, user, repo):
    """
    This unit test is useful both for manual use during development
    (i.e. to clear out a demo repo manually), _and_ as an ordinary
    unit test, which demonstrates that it is okay to call the delete
    operation on an already non-existent demo repo.
    """
    print()
    with app.app_context():
        repopath = f'demo.{user}.{repo}'
        delete_demo_repo(repopath)
