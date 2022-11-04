# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import logging
from abc import ABC, abstractmethod
from typing import cast

from .etl_base import EtlBase


class CreateOmopDb(EtlBase, ABC):
    """
    Class that creates the CDM folder structure that holds the raw queries, Usagi CSV's and custom concept CSV's.
    """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    @abstractmethod
    def run(self) -> None:
        """Create OMOP tables in the database and define indexes/partitions/clusterings"""
        pass
