#!/usr/bin/python
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

template = """\
.zoom%(z)s0 .globalZoom {
    zoom: %(z)s0%%;
    -moz-transform: scale(%(s)s);
}

.zoom%(z)s0.localZoom {
    zoom: %(z)s0%%;
    -moz-transform: scale(%(s)s);
}
"""

for r in range(5, 21):
    s = '%.1f' % (r/10.0)
    print(template % {'z': r, 's': s})

