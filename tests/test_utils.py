from __future__ import annotations

import pytest
from graphql import GraphQLInt, GraphQLString
from graphql_sqlalchemy.testing import assert_equal_gql_type


def test_failed_assertion() -> None:
    with pytest.raises(AssertionError):
        assert_equal_gql_type(GraphQLInt, GraphQLString)
