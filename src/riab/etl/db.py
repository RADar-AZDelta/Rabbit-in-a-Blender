import logging
import time
from typing import Optional

import backoff
from sqlalchemy import CursorResult, create_engine, engine, text


class Db:
    """SQLAlchemy database connection."""

    def __init__(self, url: engine.URL):
        logging.debug("Creating SQL Alchemy engine to database: %s", url)
        self._engine = create_engine(
            url,
            use_insertmanyvalues=True,
        )

    @backoff.on_exception(backoff.expo, (Exception), max_time=10, max_tries=3)
    def run_query(self, sql: str, parameters: Optional[dict] = None) -> list[dict] | None:
        """Runs a SQL query and returns the results as a list of dictionaries.

        Args:
            sql (str): The SQL query to run.
            parameters (Optional[dict]): The parameters to pass to the query.

        Returns:
            list[dict]: The results of the query as a list of dictionaries.
        """
        logging.debug("Running query: %s", sql)
        try:
            rows = None
            with self._engine.begin() as conn:
                with conn.execute(text(sql), parameters) as result:
                    if isinstance(result, CursorResult) and not result._soft_closed:
                        rows = [u._asdict() for u in result.all()]
                    return rows
        except Exception as ex:
            logging.debug("FAILED QUERY: %s", sql)
            raise ex

    def run_query_with_benchmark(
        self, sql: str, parameters: Optional[dict] = None
    ) -> tuple[Optional[list[dict]], float]:
        """Runs a SQL query and returns the results as a list of dictionaries and also the duration in time the query took to complete.

        Args:
            sql (str): The SQL query to run.
            parameters (Optional[dict]): The parameters to pass to the query.

        Returns:
            list[dict]: The results of the query as a list of dictionaries.
            float: The duration in time the query took to complete.
        """
        start = time.time()
        rows = self.run_query(sql, parameters)
        end = time.time()
        execution_time = end - start
        return rows, execution_time
