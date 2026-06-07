import datetime
import os
import pandas as pd

import great_expectations as gx
import great_expectations.exceptions as gxe
from great_expectations.validator.validator import Validator
from src import generate_dirty_inventory_data
from great_expectations.checkpoint.actions import SlackNotificationAction



context=gx.get_context()

data_source_name="product_data_source"

data_source=context.data_sources.add_pandas(name=data_source_name)

df = generate_dirty_inventory_data(n_rows=100, scenario="mixed")

data_asset=data_source.add_dataframe_asset(name="product_dataframe")

batch_definition_name = "my_batch_definition"

batch_definition=data_asset.add_batch_definition_whole_dataframe(
    name=batch_definition_name
)


suite1=context.suites.add(
    gx.ExpectationSuite(name="product_inventory_schema_validation")
)

suite2 = context.suites.add(
    gx.ExpectationSuite(name="business_rules_validation")
)

suite1.add_expectation(
    gxe.ExpectTableColumnCountToEqual(value=7)
)

suite2.add_expectation(
    gxe.ExpectColumnValuesToMatchRegex(column="product_id", regex=r"^PROD_\d{3}$")
)


validation_definition1 = gx.ValidationDefinition(
    data=batch_definition, suite=suite1, name="product_inventory_schema_validation"
)


validation_definition1 = context.validation_definitions.add(validation_definition1)
