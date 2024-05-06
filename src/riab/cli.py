# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

# pylint: disable=unsubscriptable-object
import logging
import logging.config
import sys
import traceback
from argparse import ArgumentParser, RawTextHelpFormatter
from configparser import ConfigParser
from os import environ as env
from os import getcwd, linesep, path
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Sequence, cast


class SafeConfigParser(ConfigParser):
    def safe_get(self, section, option, default=None, **kwargs):
        """Override the default get function of ConfigParser to add a default value if section or option is not found."""
        if self.has_section(section) and self.has_option(section, option):
            return ConfigParser.get(self, section, option, **kwargs)
        return default


class Cli:
    def __init__(self) -> None:
        parser = self._contstruct_argument_parser()
        args = parser.parse_args()

        with self.init_logging() as logger_file_handle:
            try:
                logging.info("Running Rabbit-in-a-Blender version %s", self._get_version())
                logging.warning("Logs are written to %s", logger_file_handle.name)
                logging.info("RiaB started with argumensts: riab %s", " ".join(sys.argv[1::]))
                if args.verbose:
                    logging.getLogger().setLevel(logging.DEBUG)

                if sys.version_info < (3, 12):
                    raise Exception("Minimum Python version is 3.12 or later")

                config = self._read_config_file(args.config)

                db_engine = config.safe_get("riab", "db_engine")
                if not db_engine:
                    raise Exception("Config file holds no db_engine option!")

                logging.debug("Database engine: %s", db_engine)
                etl_kwargs = {
                    "db_engine": db_engine.lower(),
                    "max_parallel_tables": int(cast(str, config.safe_get("riab", "max_parallel_tables", "9"))),
                    "max_worker_threads_per_table": int(
                        cast(str, config.safe_get("riab", "max_worker_threads_per_table", "16"))
                    ),
                }

                match db_engine:
                    case "bigquery":
                        bigquery_kwargs = {
                            "credentials_file": config.safe_get(db_engine, "credentials_file"),
                            "location": config.safe_get(db_engine, "location"),
                            "project_raw": config.safe_get(db_engine, "project_raw"),
                            "dataset_work": config.safe_get(db_engine, "dataset_work", "work"),
                            "dataset_omop": config.safe_get(db_engine, "dataset_omop", "omop"),
                            "dataset_dqd": config.safe_get(db_engine, "dataset_dqd", "dqd"),
                            "dataset_achilles": config.safe_get(db_engine, "dataset_achilles", "achilles"),
                            "bucket": config.safe_get(db_engine, "bucket"),
                        }
                    case "sql_server":
                        sqlserver_kwargs = {
                            "server": config.safe_get(db_engine, "server"),
                            "user": config.safe_get(db_engine, "user"),
                            "password": config.safe_get(db_engine, "password"),
                            "port": cast(int, config.safe_get(db_engine, "port", "1433")),
                            "raw_database_catalog": config.safe_get(db_engine, "raw_database_catalog", "raw"),
                            "raw_database_schema": config.safe_get(db_engine, "raw_database_schema", "dbo"),
                            "omop_database_catalog": config.safe_get(db_engine, "omop_database_catalog", "omop"),
                            "omop_database_schema": config.safe_get(db_engine, "omop_database_schema", "dbo"),
                            "work_database_catalog": config.safe_get(db_engine, "work_database_catalog", "work"),
                            "work_database_schema": config.safe_get(db_engine, "work_database_schema", "dbo"),
                            "dqd_database_catalog": config.safe_get(db_engine, "dqd_database_catalog", "dqd"),
                            "dqd_database_schema": config.safe_get(db_engine, "dqd_database_schema", "dbo"),
                            "achilles_database_catalog": config.safe_get(
                                db_engine, "achilles_database_catalog", "achilles"
                            ),
                            "achilles_database_schema": config.safe_get(db_engine, "achilles_database_schema", "dbo"),
                            "disable_fk_constraints": cast(
                                str, config.safe_get(db_engine, "disable_fk_constraints", "false")
                            ).lower()
                            in ["true", "1", "yes"],
                            "bcp_code_page": config.safe_get(db_engine, "bcp_code_page", "ACP"),
                        }
                    case _:
                        raise ValueError("Not a supported database engine: '{db_engine}'")

                if args.print_etl_flow:
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.etl import BigQueryEtl

                            etl = BigQueryEtl(
                                **etl_kwargs,
                                **bigquery_kwargs,
                            )
                            logging.info(
                                "Resolved ETL tables foreign keys dependency graph: \n%s",
                                etl.print_cdm_tables_fks_dependencies_tree(),
                            )
                        case "sql_server":
                            from .etl.sql_server.etl import SqlServerEtl

                            etl = SqlServerEtl(
                                **etl_kwargs,
                                **sqlserver_kwargs,
                            )
                            logging.info(
                                "Resolved ETL tables foreign keys dependency graph: \n%s",
                                etl.print_cdm_tables_fks_dependencies_tree(),
                            )
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.create_db:  # create OMOP CDM Database
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.create_omop_db import BigQueryCreateOmopDb

                            with BigQueryCreateOmopDb(
                                **etl_kwargs,
                                **bigquery_kwargs,
                            ) as create_omop_db:
                                create_omop_db.run()
                        case "sql_server":
                            from .etl.sql_server.create_omop_db import SqlServerCreateOmopDb

                            with SqlServerCreateOmopDb(
                                **etl_kwargs,
                                **sqlserver_kwargs,
                            ) as create_omop_db:
                                create_omop_db.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.test_db_connection:
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.etl import BigQueryEtl

                            etl = BigQueryEtl(
                                **etl_kwargs,
                                **bigquery_kwargs,
                            )
                            etl._test_db_connection()
                        case "sql_server":
                            from .etl.sql_server.etl import SqlServerEtl

                            etl = SqlServerEtl(
                                **etl_kwargs,
                                **sqlserver_kwargs,
                            )
                            etl._test_db_connection()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.create_folders:  # create the ETL folder structure
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.create_cdm_folders import BigQueryCreateCdmFolders

                            with BigQueryCreateCdmFolders(
                                **etl_kwargs,
                                cdm_folder_path=args.run_etl or args.create_folders,
                                **bigquery_kwargs,
                            ) as create_folders:
                                create_folders.run()
                        case "sql_server":
                            from .etl.sql_server.create_cdm_folders import SqlServerCreateCdmFolders

                            with SqlServerCreateCdmFolders(
                                **etl_kwargs,
                                cdm_folder_path=args.run_etl or args.create_folders,
                                **sqlserver_kwargs,
                            ) as create_folders:
                                create_folders.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.import_vocabularies:  # impoprt OMOP CDM vocabularies
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.import_vocabularies import BigQueryImportVocabularies

                            with BigQueryImportVocabularies(
                                **etl_kwargs,
                                **bigquery_kwargs,
                            ) as import_vocabularies:
                                import_vocabularies.run(args.import_vocabularies)
                        case "sql_server":
                            from .etl.sql_server.import_vocabularies import SqlServerImportVocabularies

                            with SqlServerImportVocabularies(
                                **etl_kwargs,
                                **sqlserver_kwargs,
                            ) as import_vocabularies:
                                import_vocabularies.run(args.import_vocabularies)
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.run_etl:  # run ETL
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.etl import BigQueryEtl

                            with BigQueryEtl(
                                **etl_kwargs,
                                cdm_folder_path=args.run_etl or args.create_folders,
                                only_omop_table=args.table,
                                only_query=args.only_query,
                                skip_usagi_and_custom_concept_upload=args.skip_usagi_and_custom_concept_upload,
                                process_semi_approved_mappings=args.process_semi_approved_mappings,
                                skip_event_fks_step=args.skip_event_fks_step,
                                **bigquery_kwargs,
                            ) as etl:
                                etl.run()
                        case "sql_server":
                            from .etl.sql_server.etl import SqlServerEtl

                            with SqlServerEtl(
                                **etl_kwargs,
                                cdm_folder_path=args.run_etl or args.create_folders,
                                only_omop_table=args.table,
                                only_query=args.only_query,
                                skip_usagi_and_custom_concept_upload=args.skip_usagi_and_custom_concept_upload,
                                process_semi_approved_mappings=args.process_semi_approved_mappings,
                                skip_event_fks_step=args.skip_event_fks_step,
                                **sqlserver_kwargs,
                            ) as etl:
                                etl.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.cleanup:  # cleanup OMOP DB
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.cleanup import BigQueryCleanup

                            with BigQueryCleanup(
                                **etl_kwargs,
                                cdm_folder_path=args.run_etl or args.create_folders,
                                clear_auto_generated_custom_concept_ids=args.clear_auto_generated_custom_concept_ids,
                                **bigquery_kwargs,
                            ) as cleanup:
                                cleanup.run(args.cleanup)
                        case "sql_server":
                            from .etl.sql_server.cleanup import SqlServerCleanup

                            with SqlServerCleanup(
                                **etl_kwargs,
                                cdm_folder_path=args.run_etl or args.create_folders,
                                clear_auto_generated_custom_concept_ids=args.clear_auto_generated_custom_concept_ids,
                                **sqlserver_kwargs,
                            ) as cleanup:
                                cleanup.run(args.cleanup)
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.data_quality:  # check data quality
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.data_quality import BigQueryDataQuality

                            with BigQueryDataQuality(
                                **etl_kwargs,
                                cdm_folder_path=args.run_etl or args.create_folders,
                                json_path=args.json,
                                **bigquery_kwargs,
                            ) as data_quality:
                                data_quality.run()
                        case "sql_server":
                            from .etl.sql_server.data_quality import SqlServerDataQuality

                            with SqlServerDataQuality(
                                **etl_kwargs,
                                cdm_folder_path=args.run_etl or args.create_folders,
                                json_path=args.json,
                                **sqlserver_kwargs,
                            ) as data_quality:
                                data_quality.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.data_quality_dashboard:  # view data quality results
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.data_quality_dashboard import BigQueryDataQualityDashboard

                            with BigQueryDataQualityDashboard(
                                **etl_kwargs,
                                dqd_port=args.port if args.port else 8050,
                                **bigquery_kwargs,
                            ) as data_quality_dashboard:
                                data_quality_dashboard.run()
                        case "sql_server":
                            from .etl.sql_server.data_quality_dashboard import SqlServerDataQualityDashboard

                            with SqlServerDataQualityDashboard(
                                **etl_kwargs,
                                dqd_port=args.port if args.port else 8050,
                                **sqlserver_kwargs,
                            ) as data_quality_dashboard:
                                data_quality_dashboard.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.achilles:  # run descriptive statistics
                    match db_engine:
                        case "bigquery":
                            from .etl.bigquery.achilles import BigQueryAchilles

                            with BigQueryAchilles(
                                **etl_kwargs,
                                **bigquery_kwargs,
                            ) as achilles:
                                achilles.run()
                        case "sql_server":
                            from .etl.sql_server.achilles import SqlServerAchilles

                            with SqlServerAchilles(
                                **etl_kwargs,
                                **sqlserver_kwargs,
                            ) as achilles:
                                achilles.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                else:
                    raise Exception("Unknown ETL command!")

            except Exception:
                logging.error(traceback.format_exc())
                if __debug__:
                    breakpoint()
            finally:
                logging.warning("Logs were written to %s", logger_file_handle.name)

    def _read_config_file(self, args_config: str) -> SafeConfigParser:
        from dotenv import load_dotenv

        # load the ini config file by using the cascade:
        # 1) check if passed as an argument
        # 2) check if passed as the RIAB_CONFIG environment variable
        # 3) check if a riab.ini exists in the current folder
        ini_config_path: str | None = args_config
        if not ini_config_path:
            load_dotenv()  # take environment variables from .env.
            ini_config_path = env.get("RIAB_CONFIG")
        if not ini_config_path:
            ini_config_path = path.join(getcwd(), "riab.ini")
            if not path.exists(ini_config_path):
                ini_config_path = None

        if not ini_config_path:
            raise Exception("No config file provided!")

        config = SafeConfigParser()
        config.read(ini_config_path)

        return config

    def _get_version(self) -> str:
        try:
            if "debugpy" in sys.modules:
                import tomllib

                with open("pyproject.toml", "rb") as f:
                    pyproject_data = tomllib.load(f)
                    return pyproject_data["project"]["version"]
        except Exception:
            pass

        from importlib import metadata

        return metadata.version("Rabbit-in-a-Blender")

    def _create_default_options_argument_parser(self) -> ArgumentParser:
        """Argument parser for the required named arguments"""

        parser = ArgumentParserWithBetterErrorPrinting(add_help=False, formatter_class=RawTextHelpFormatter)
        parser.add_argument(
            "-v",
            "--verbose",
            help="Verbose logging (logs are also writen to a log file in the systems tmp folder)",
            action="store_true",
        )

        parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=rf"""
______      _     _     _ _     _                ______ _                _           
| ___ \    | |   | |   (_) |   (_)               | ___ \ |              | |          
| |_/ /__ _| |__ | |__  _| |_   _ _ __     __ _  | |_/ / | ___ _ __   __| | ___ _ __ 
|    // _` | '_ \| '_ \| | __| | | '_ \   / _` | | ___ \ |/ _ \ '_ \ / _` |/ _ \ '__|
| |\ \ (_| | |_) | |_) | | |_  | | | | | | (_| | | |_/ / |  __/ | | | (_| |  __/ |   
\_| \_\__,_|_.__/|_.__/|_|\__| |_|_| |_|  \__,_| \____/|_|\___|_| |_|\__,_|\___|_|   

                            VERSION     {self._get_version()}    
                                                                                
                                  ,/,(((                                        
                                  *((((((.                                      
                                  ,(((((((,,,.                                  
                                    *((((((((((((.                              
                                   /(((((((((/,((((.                            
                                   .((((((((((((((((                            
                                  ,((((((((((((*..                              
                               ,(((((((((((((((,                                
                            *((((((((((((((((((/                                
                      @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&                           
                      @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&@@.                      
                      @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&  #&&&&                    
                      @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&    @&&                    
                      @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&@    @&&                    
                      @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&@    @&&                    
                      @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&@    @&&                    
                      &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&@    @&&                    
                      #&&&&&&&&&&&&&&&&&&&&&&&&&&&&&@    @&&                    
                      /&&&&&&&&&&&&&&&&&&&&&&&&&&&&&@    @&&                    
                       &&&&&&&&&&&&&&&&&&&&&&&&&&&&&% *@&&@*                    
                       @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&@,                       
                       ,&&&&&&&&&&&&&&&&&&&&&&&&&&&&&                           
                        ,&&&&&&&&&&&&&&&&&&&&&&&&&&                             
                          @&&&&&&&&&&&&&&&&&&&&&&*                              
                           #&&&&&&&&&&&&&&&&&&&@                                
                             @&&&&&&&&&&&&&&&&,                                 
                             (((((((((((((((((,                                 
                           %&&&&&&&&&&&&&&&&&&&&                                
                          &&&    /&/    @@    %&@.                              
                         @&&&&&&&&&@(.*@@&&&&&&&&&                              
                        %&&&&&&&&   @&&&# ,&&&&&&&&                             
                       &&&&&&&&& &&&# #&&@ /&&&&&&&@                            
                      (&&&&&&&&& .&&&&&&&% %&&&&&&&&@                           
                      @&&&&&&&&&&&  ,/*  /&&&&&&&&&&&                           
                      @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&                           
                                       
            """,
        )

        optional_argument_group = parser.add_argument_group("Optional named arguments")
        optional_argument_group.add_argument(
            "--config",
            help="""Optional path to the ini config file that holds the database engine configuration.
            Alternatively set the RIAB_CONFIG environment variable, pointing to the ini file.
            Or place a riab.ini file in the current directory.
            """,
            nargs="?",
            type=str,
            metavar="PATH",
            # required=not bool(set(sys.argv) & {"-h", "--help"}),
        )

        args, _ = parser.parse_known_args()
        return parser

    def _create_etl_command_argument_parser(self, parents: Sequence[ArgumentParser]) -> ArgumentParser:
        """Argument parser for the ETL commands"""

        parser = ArgumentParserWithBetterErrorPrinting(add_help=False, parents=parents)
        argument_group = parser.add_argument_group("ETL commands")
        argument_group.add_argument("-cd", "--create-db", help="Create the OMOP CDM tables", action="store_true")
        argument_group.add_argument(
            "-tdc", "--test-db-connection", help="Test connection to the database", action="store_true"
        )
        argument_group.add_argument(
            "-cf",
            "--create-folders",
            help="Create the ETL folder structure that will hold your queries, Usagi CSV's an custom concept CSV's.",
            nargs="?",
            type=str,
            metavar="PATH",
        )
        argument_group.add_argument(
            "-i",
            "--import-vocabularies",
            nargs="?",
            type=str,
            help="""Extracts the vocabulary zip file (downloaded from the Athena website) and imports it
            into the OMOP CDM database.""",
            metavar="VOCABULARIES_ZIP_FILE",
        )
        argument_group.add_argument(
            "-r",
            "--run-etl",
            help="Runs the ETL, pass the path to ETL folder structure that holds your queries, Usagi CSV's an custom concept CSV's.",  # noqa: E501 # pylint: disable=line-too-long
            nargs="?",
            type=str,
            metavar="PATH",
        )
        argument_group.add_argument(
            "-c",
            "--cleanup",
            nargs="?",
            const="all",
            choices=[
                "all",
                "metadata",
                "cdm_source",
                "vocabulary",
                "location",
                "care_site",
                "provider",
                "person",
                "observation_period",
                "visit_occurrence",
                "visit_detail",
                "condition_occurrence",
                "drug_exposure",
                "procedure_occurrence",
                "device_exposure",
                "measurement",
                "observation",
                "death",
                "note",
                "note_nlp",
                "specimen",
                "fact_relationship",
                "payer_plan_period",
                "cost",
                "episode",
                "episode_event",
            ],
            type=str,
            help="""Cleanup all the OMOP tables (ALL or no TABLE parameter), or just one.
            Be aware that the cleanup of a single table can screw up foreign keys!
            For instance cleaning up only the 'Person' table,
            will result in clicical results being mapped to the wrong persons!!!!""",
            metavar="TABLE",
        )
        argument_group.add_argument(
            "-dq",
            "--data-quality",
            help="Check the data quality.",
            action="store_true",
        )
        argument_group.add_argument(
            "-dqd",
            "--data-quality-dashboard",
            help="View the data quality results.",
            action="store_true",
        )
        argument_group.add_argument(
            "-ach",
            "--achilles",
            help="Generate the descriptive statistics.",
            action="store_true",
        )
        argument_group.add_argument(
            "--print-etl-flow",
            help="Print the sequence in which the ETL tables that will be processed",
            action="store_true",
        )

        command_args, _ = parser.parse_known_args()
        return parser

    def _create_etl_command_argument_group(self, parser: ArgumentParser):
        argument_group = parser.add_argument_group("Run ETL specific command options (-r [PATH], --run-etl [PATH])")

        argument_group.add_argument(
            "-se",
            "--skip-event-fks-step",
            help="""Skip the event foreign keys ETL step.""",
            action="store_true",
        )
        argument_group.add_argument(
            "-sa",
            "--process-semi-approved-mappings",
            help="""In addition to 'APPROVED' as mapping status, 'SEMI-APPROVED' will be processed as valid Usagi concept mappings.""",
            action="store_true",
        )
        argument_group.add_argument(
            "-s",
            "--skip-usagi-and-custom-concept-upload",
            help="""Skips the parsing and uploading of the Usagi and custom concept CSV's.
            This can be usefull if you only want to upload aditional data to the OMOP database, and there were no changes
            to the Usagi CSV's an custom concept CSV's.
            Skipping results in a significant speed boost.""",
            action="store_true",
        )
        argument_group.add_argument(
            "-q",
            "--only-query",
            help="Run only the specified ETL query(s) form the CMD folder structure (ex: measurement/lab_measurements.sql).",  # noqa: E501 # pylint: disable=line-too-long
            nargs="?",
            type=str,
            metavar="PATH",
            action="append",
        )
        argument_group.add_argument(
            "-t",
            "--table",
            nargs="?",
            choices=[
                "metadata",
                "cdm_source",
                "vocabulary",
                "location",
                "care_site",
                "provider",
                "person",
                "observation_period",
                "visit_occurrence",
                "visit_detail",
                "condition_occurrence",
                "drug_exposure",
                "procedure_occurrence",
                "device_exposure",
                "measurement",
                "observation",
                "death",
                "note",
                "note_nlp",
                "specimen",
                "fact_relationship",
                "payer_plan_period",
                "cost",
                "episode",
                "episode_event",
            ],
            type=str,
            help="""Do only ETL on this specific OMOP CDM table""",
            metavar="TABLE",
            action="append",
        )

    def _create_cleanup_command_argument_group(self, parser: ArgumentParser):
        argument_group = parser.add_argument_group("Cleanup specific arguments")
        argument_group.add_argument(
            "--clear-auto-generated-custom-concept-ids",
            help="""Cleanup the auto generated custom concept ID's (above 2 million). Without this argument, the cleanup command will not clear the mapping table (that maps the custom concept with the auto generated id above 2 million), so that you can use those above 2 million concept id's in your cohort builder, without the fear that those id's will change.""",  # noqa: E501 # pylint: disable=line-too-long
            action="store_true",
        )

    def _create_data_quality_command_argument_group(self, parser: ArgumentParser):
        argument_group = parser.add_argument_group("Data quality specific arguments")
        argument_group.add_argument(
            "--json",
            nargs="?",
            type=str,
            help="""Path to store the data quality result as JSON file.""",
            metavar="PATH",
        )

    def _create_data_quality_dashboard_command_argument_group(self, parser: ArgumentParser):
        argument_group = parser.add_argument_group("Data quality dashboard specific arguments")
        argument_group.add_argument(
            "--port",
            nargs="?",
            type=int,
            help="""Port where the data quality dashboard schould listen on.""",
            metavar="PORT",
        )

    def _contstruct_argument_parser(self) -> ArgumentParser:
        """Constructs the argument parser"""

        default_options_argument_parser = self._create_default_options_argument_parser()
        etl_command_argument_parser = self._create_etl_command_argument_parser(
            parents=[default_options_argument_parser]
        )

        parser = ArgumentParserWithBetterErrorPrinting(
            prog="riab",
            description="Rabbit in a Blender (RiaB): an OMOP CDM ETL tool",
            parents=[etl_command_argument_parser],
        )

        self._create_etl_command_argument_group(parser)
        self._create_cleanup_command_argument_group(parser)
        self._create_data_quality_command_argument_group(parser)
        self._create_data_quality_dashboard_command_argument_group(parser)

        return parser

    def init_logging(self) -> _TemporaryFileWrapper:
        """Initialise logging"""
        # get main logger
        main_logger = logging.getLogger()
        main_logger.setLevel(logging.INFO)

        # formatters
        console_formatter = ColoredFormatter("%(asctime)s: %(name)s: #%(lineno)d: %(levelname)s - %(message)s")
        file_formatter = logging.Formatter("%(asctime)s %(levelname)s %(pathname)s#%(lineno)d %(message)s")

        # console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        main_logger.addHandler(console_handler)

        # file handler
        tmp_file_handle = NamedTemporaryFile(delete=False, prefix="riab_", suffix=".log")
        file_handler = logging.FileHandler(tmp_file_handle.name)
        file_handler.setFormatter(file_formatter)
        main_logger.addHandler(file_handler)

        return tmp_file_handle


class ArgumentParserWithBetterErrorPrinting(ArgumentParser):
    """Custom ArgumentParser class with better error printing"""

    def error(self, message):
        """Prints a help message to stdout, the error message to stderr and
        exits.

        Args:
            message (string): The error message
        """
        self.print_help()
        sys.stderr.write(f"error: {message}{linesep}")
        sys.exit(2)


class ColoredFormatter(logging.Formatter):
    """Logging colored formatter, adapted from https://stackoverflow.com/a/56944256/3638629"""

    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.blue + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.bold_red + self.fmt + self.reset,
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
