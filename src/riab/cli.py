# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

# pylint: disable=unsubscriptable-object
import logging
import logging.config
import sys
import traceback
from argparse import ArgumentParser, RawTextHelpFormatter
from configparser import ConfigParser
from importlib import metadata
from os import environ as env
from os import getcwd, linesep, path
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Optional, Sequence

from dotenv import load_dotenv

from .etl import (
    Achilles,
    Cleanup,
    CreateCdmFolders,
    CreateOmopDb,
    DataQuality,
    DataQualityDashboard,
    Etl,
    ImportVocabularies,
)
from .etl.bigquery import (
    BigQueryAchilles,
    BigQueryCleanup,
    BigQueryCreateCdmFolders,
    BigQueryCreateOmopDb,
    BigQueryDataQuality,
    BigQueryDataQualityDashboard,
    BigQueryEtl,
    BigQueryImportVocabularies,
)


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
                logging.warning("Logs are written to %s", logger_file_handle.name)
                if __debug__:
                    print(args)
                if args.verbose:
                    logging.getLogger().setLevel(logging.DEBUG)

                config = self._read_config_file(args.config)

                db_engine = config.safe_get("RiaB", "db_engine")
                if not db_engine:
                    raise Exception("Config file holds no db_engine option!")

                bigquery_kwargs = None
                match db_engine:
                    case "BigQuery":
                        bigquery_kwargs = {
                            "credentials_file": config.safe_get(db_engine, "credentials_file"),
                            "location": config.safe_get(db_engine, "location"),
                            "project_raw": config.safe_get(db_engine, "project_raw"),
                            "dataset_work": config.safe_get(db_engine, "dataset_work"),
                            "dataset_omop": config.safe_get(db_engine, "dataset_omop"),
                            "dataset_dqd": config.safe_get(db_engine, "dataset_dqd"),
                            "dataset_achilles": config.safe_get(db_engine, "dataset_achilles"),
                            "bucket": config.safe_get(db_engine, "bucket"),
                        }
                    case _:
                        raise ValueError("Not a supported database engine: '{db_engine}'")

                if args.create_db:  # create OMOP CDM Database
                    create_omop_db: Optional[CreateOmopDb] = None
                    match db_engine:
                        case "BigQuery":
                            create_omop_db = BigQueryCreateOmopDb(
                                cdm_folder_path=args.run_etl or args.create_folders,
                                **bigquery_kwargs,
                            )
                            create_omop_db.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.create_folders:  # create the ETL folder structure
                    create_folders: Optional[CreateCdmFolders] = None
                    match db_engine:
                        case "BigQuery":
                            create_folders = BigQueryCreateCdmFolders(
                                cdm_folder_path=args.run_etl or args.create_folders,
                                **bigquery_kwargs,
                            )
                            create_folders.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.import_vocabularies:  # impoprt OMOP CDM vocabularies
                    import_vocabularies: Optional[ImportVocabularies] = None
                    match db_engine:
                        case "BigQuery":
                            import_vocabularies = BigQueryImportVocabularies(
                                cdm_folder_path=args.run_etl or args.create_folders,
                                **bigquery_kwargs,
                            )
                            import_vocabularies.run(args.import_vocabularies)
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.run_etl:  # run ETL
                    etl: Optional[Etl] = None
                    match db_engine:
                        case "BigQuery":
                            etl = BigQueryEtl(
                                cdm_folder_path=args.run_etl or args.create_folders,
                                only_omop_table=args.table,
                                only_query=args.only_query,
                                skip_usagi_and_custom_concept_upload=args.skip_usagi_and_custom_concept_upload,
                                process_semi_approved_mappings=args.process_semi_approved_mappings,
                                **bigquery_kwargs,
                            )
                            etl.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.cleanup:  # cleanup OMOP DB
                    cleanup: Optional[Cleanup] = None
                    match db_engine:
                        case "BigQuery":
                            cleanup = BigQueryCleanup(
                                cdm_folder_path=args.run_etl or args.create_folders,
                                clear_auto_generated_custom_concept_ids=args.clear_auto_generated_custom_concept_ids,
                                **bigquery_kwargs,
                            )
                            cleanup.run(args.cleanup)
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.data_quality:  # check data quality
                    data_quality: Optional[DataQuality] = None
                    match db_engine:
                        case "BigQuery":
                            data_quality = BigQueryDataQuality(
                                cdm_folder_path=args.run_etl or args.create_folders,
                                json_path=args.json,
                                **bigquery_kwargs,
                            )
                            data_quality.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.data_quality_dashboard:  # view data quality results
                    data_quality_dashboard: Optional[DataQualityDashboard] = None
                    match db_engine:
                        case "BigQuery":
                            data_quality_dashboard = BigQueryDataQualityDashboard(
                                port=args.port if args.port else 8050,
                                **bigquery_kwargs,
                            )
                            data_quality_dashboard.run()
                        case _:
                            raise ValueError("Not a supported database engine")
                elif args.achilles:  # run descriptive statistics
                    achilles: Optional[Achilles] = None
                    match db_engine:
                        case "BigQuery":
                            achilles = BigQueryAchilles(
                                scratch_database_schema=args.bigquery_dataset_id_work,
                                results_database_schema=args.bigquery_dataset_id_omop,
                                cdm_database_schema=args.bigquery_dataset_id_omop,
                                temp_emulation_schema=args.bigquery_dataset_id_work,
                                **bigquery_kwargs,
                            )
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
            # version=f"Version: {metadata.version('Rabbit-in-a-Blender')}"
            version=rf"""
______      _     _     _ _     _                ______ _                _           
| ___ \    | |   | |   (_) |   (_)               | ___ \ |              | |          
| |_/ /__ _| |__ | |__  _| |_   _ _ __     __ _  | |_/ / | ___ _ __   __| | ___ _ __ 
|    // _` | '_ \| '_ \| | __| | | '_ \   / _` | | ___ \ |/ _ \ '_ \ / _` |/ _ \ '__|
| |\ \ (_| | |_) | |_) | | |_  | | | | | | (_| | | |_/ / |  __/ | | | (_| |  __/ |   
\_| \_\__,_|_.__/|_.__/|_|\__| |_|_| |_|  \__,_| \____/|_|\___|_| |_|\__,_|\___|_|   

                            VERSION     {metadata.version('Rabbit-in-a-Blender')}    
                                                                                
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

        command_args, _ = parser.parse_known_args()
        return parser

    def _create_etl_command_argument_group(self, parser: ArgumentParser):
        argument_group = parser.add_argument_group("Run ETL specific command options (-r [PATH], --run-etl [PATH])")

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
            help="Run only the specified ETL query form the CMD folder structure (ex: measurement/lab_measurements.sql).",  # noqa: E501 # pylint: disable=line-too-long
            nargs="?",
            type=str,
            metavar="PATH",
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
        tmp_file_handle = NamedTemporaryFile(delete=False, prefix="omop_etl_", suffix=".log")
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
