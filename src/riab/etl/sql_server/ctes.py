from typing import Any, cast
from sqlparse import parse
from sqlparse.tokens import CTE
from sqlparse.sql import Identifier, IdentifierList, Parenthesis

def extract_ctes(sql):
    """ Extract constant table expresseions from a query"""

    p = parse(sql)[0]

    # Make sure the first meaningful token is "WITH" which is necessary to
    # define CTEs
    idx, tok = cast(tuple[int, Any], p.token_next(-1, skip_ws=True, skip_cm=True))
    if not (tok and tok.ttype == CTE):
        return "", sql

    # Get the next (meaningful) token, which should be the first CTE
    idx, tok = cast(tuple[int, Any], p.token_next(idx))
    if not tok:
        return ("", "")
    start_pos = _token_start_pos(p.tokens, idx)
    ctes = []

    if isinstance(tok, IdentifierList):
        # Multiple ctes
        for t in tok.get_identifiers():
            cte_start_offset = _token_start_pos(tok.tokens, tok.token_index(t))
            cte = _get_cte_from_token(t, start_pos + cte_start_offset)
            if not cte:
                continue
            ctes.append(cte)
    elif isinstance(tok, Identifier):
        # A single CTE
        cte = _get_cte_from_token(tok, start_pos)
        if cte:
            ctes.append(cte)

    idx = p.token_index(tok) + 1

    # Collapse everything after the ctes into a remainder query
    remainder = u"".join(str(tok) for tok in p.tokens[idx:])

    ctes = "WITH " + ", ".join(ctes) if ctes else ""

    return ctes, remainder


def _get_cte_from_token(tok, pos0):
    cte_name = tok.get_real_name()
    if not cte_name:
        return None

    # Find the start position of the opening parens enclosing the cte body
    idx, parens = tok.token_next_by(Parenthesis)
    if not parens:
        return None

    cte_value = parens.value

    return f"{cte_name} AS {cte_value}"

def _token_start_pos(tokens, idx):
    return sum(len(str(t)) for t in tokens[:idx])