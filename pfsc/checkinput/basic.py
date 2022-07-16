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

from pfsc.excep import PfscExcep, PECode

def check_boolean(key, raw, typedef):
    """
    :param raw: either an actual boolean value, or else a string s
                such that (ideally) s.lower() in ['true', 'false'];
                but this is not really checked -- see return value
    :param typedef:
        accept_int: boolean
            If True, accept int or string that parses to int, and take 0 to
            mean False, and any other integer to mean True.
    :return: an actual boolean which is True if s is True or s.lower() == 'true', and False otherwise
    """
    if raw is True or raw is False:
        return raw
    if typedef.get('accept_int'):
        try:
            n = int(raw)
        except ValueError:
            pass
        else:
            return n != 0
    if isinstance(raw, str):
        return raw.lower() == 'true'
    return False

def check_integer(key, raw, typedef):
    """
    :param raw: either an actual integer, or a string rep of integer, base 10
    :param typedef:
        opt:
            min: minimum integer value accepted (inclusive)
            max: maximum integer value accepted (inclusive)
            default_on_empty: value to take as default in case of empty string input
            divisors: list of integers that must divide the given one
    :return: int
    """
    lb = typedef.get('min')
    ub = typedef.get('max')
    if 'default_on_empty' in typedef and raw == '':
        return typedef['default_on_empty']
    try:
        n = int(raw)
    except Exception:
        raise PfscExcep('Bad integer', PECode.BAD_INTEGER, bad_field=key)
    try:
        if lb is not None:
            assert n >= lb
        if ub is not None:
            assert n <= ub
    except Exception:
        raise PfscExcep('Integer out of range', PECode.BAD_INTEGER, bad_field=key)

    divisors = typedef.get('divisors', [])
    for d in divisors:
        if n % d != 0:
            raise PfscExcep(f'Integer must be divisible by {d}', PECode.BAD_INTEGER, bad_field=key)

    return n

def check_simple_dict(key, raw, typedef):
    """
    The "simple" in the name of this function contrasts it with the `check_dict`
    function which requires a formal typedef for keys and for values. Here, you
    may provide a Python type for keys and for values, but each is optional.

    :param raw: an actual dictionary or a string rep thereof
    :param typedef:
        opt:
            keytype: a Python type of which all keys in the dict must be instances
            valtype: a Python type of which all values in the dict must be instances
    :return: dict
    """
    if isinstance(raw, str):
        try:
            d = dict(raw)
        except Exception:
            raise PfscExcep('Bad dictionary', PECode.INPUT_WRONG_TYPE, bad_field=key)
    else:
        d = raw
    if not isinstance(d, dict):
        raise PfscExcep('Bad dictionary', PECode.INPUT_WRONG_TYPE, bad_field=key)
    kt = typedef.get('keytype')
    vt = typedef.get('valtype')
    if kt is not None:
        for k in d.keys():
            if not isinstance(k, kt):
                raise PfscExcep('Key of wrong type in dictionary', PECode.INPUT_WRONG_TYPE, bad_field=key)
    if vt is not None:
        for v in d.values():
            if not isinstance(v, vt):
                raise PfscExcep('Value of wrong type in dictionary', PECode.INPUT_WRONG_TYPE, bad_field=key)
    return d

def check_string(key, raw, typedef):
    """
    Check that the input is a string.
    See Pfsc-7.1 for more sophisticated checks we might want here.

    :param typedef:
        optional:
            values: list of strings giving the only allowed values
            max_len: maximum allowable length of the string
    """
    if not isinstance(raw, str):
        raise PfscExcep('Expecting string', PECode.INPUT_WRONG_TYPE, bad_field=key)

    values = typedef.get('values')
    if values is not None:
        if raw not in values:
            raise PfscExcep('Param not among allowed values.', PECode.INPUT_WRONG_TYPE, bad_field=key)

    max_len = typedef.get('max_len')
    if isinstance(max_len, int):
        if len(raw) > max_len:
            raise PfscExcep('String too long.', PECode.INPUT_TOO_LONG, bad_field=key)

    return raw

def check_json(key, raw, typedef):
    """
    Check that the input parses as JSON.

    :param typedef: nothing special
    :return: the parsed object
    """
    import json
    try:
        obj = json.loads(raw)
    except json.decoder.JSONDecodeError:
        raise PfscExcep('Malformed JSON', PECode.MALFORMED_JSON, bad_field=key)
    return obj
