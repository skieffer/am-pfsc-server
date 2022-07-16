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

import re

from lark import Lark
import lark.exceptions

from pfsc.excep import PfscExcep, PECode

NONEMPTY_HEXADECIMAL_PATTERN = re.compile(r'^[a-fA-F0-9]+$')

def check_pdf_fingerprint(key, raw, typedef):
    """
    A PDF fingerprint is a hexadecimal hash computed by Mozilla's pdf.js
    in order to (probably uniquely) identify a PDF document.
    """
    if not isinstance(raw, str):
        raise PfscExcep('Malformed PDF Fingerprint', PECode.MALFORMED_PDF_FINGERPRINT, bad_field=key)
    if not NONEMPTY_HEXADECIMAL_PATTERN.match(raw):
        raise PfscExcep('Malformed PDF Fingerprint', PECode.MALFORMED_PDF_FINGERPRINT, bad_field=key)
    return raw

combiner_code_grammar = r"""
    program: version scale content_command+
    version: "v" (DECIMAL|INT) ("." INT)* ";"?
    scale: "s" (DECIMAL|INT) ";"?
    ?content_command: box | x_shift | y_shift | newline
    box: "(" INT ":" INT ":" INT ":" INT ":" INT ":" INT ":" INT ")" ";"?
    x_shift: "x" SIGNED_INT ";"?
    y_shift: "y" SIGNED_INT ";"?
    newline: "n" ";"?
    SIGNED_INT: ("+"|"-") INT
    
    %import common.INT
    %import common.DECIMAL
    %import common.WS
    %ignore WS
"""

combiner_code_parser = Lark(combiner_code_grammar, start='program', parser='lalr', lexer='standard')

def check_combiner_code(key, raw, typedef):
    """
    A combiner code is a little program used to indicate a way of selecting and
    combining boxes (from a PDF document).

    typedef:
        opt:
            version: Which version of combiner code are we checking? Defaults to 2.

    Note: For now we are only capable of checking version 2.
    """
    desired_version = typedef.get('version', 2)
    if desired_version != 2:
        msg = f'Trying to check unknown combiner code version: {desired_version}'
        raise PfscExcep(msg, PECode.PDF_COMBINER_CODE_UKNOWN_VERS, bad_field=key)
    desired_v_code = f'v{desired_version}'

    try:
        combiner_code_parser.parse(raw)
    except lark.exceptions.LarkError as e:
        msg = f'Malformed combiner code: {e}'
        raise PfscExcep(msg, PECode.MALFORMED_COMBINER_CODE, bad_field=key)

    commands = raw.split(';')
    if commands[0] != desired_v_code:
        msg = f'Combiner code of unknown version: {commands[0]}'
        raise PfscExcep(msg, PECode.PDF_COMBINER_CODE_UKNOWN_VERS, bad_field=key)

    return raw
