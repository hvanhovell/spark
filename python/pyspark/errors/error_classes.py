#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json


ERROR_CLASSES_JSON = """
{
  "APPLICATION_NAME_NOT_SET" : {
    "message" : [
      "An application name must be set in your configuration."
    ]
  },
  "ARGUMENT_REQUIRED": {
    "message": [
      "Argument `<arg_name>` is required when <condition>."
    ]
  },
  "ATTRIBUTE_NOT_CALLABLE" : {
    "message" : [
      "Attribute `<attr_name>` in provided object `<obj_name>` is not callable."
    ]
  },
  "ATTRIBUTE_NOT_SUPPORTED" : {
    "message" : [
      "Attribute `<attr_name>` is not supported."
    ]
  },
  "AXIS_LENGTH_MISMATCH" : {
    "message" : [
      "Length mismatch: Expected axis has <expected_length> element, new values have <actual_length> elements."
    ]
  },
  "BROADCAST_VARIABLE_NOT_LOADED": {
    "message": [
      "Broadcast variable `<variable>` not loaded."
    ]
  },
  "CALL_BEFORE_INITIALIZE": {
    "message": [
      "Not supported to call `<func_name>` before initialize <object>."
    ]
  },
  "CANNOT_ACCEPT_OBJECT_IN_TYPE": {
    "message": [
      "`<data_type>` can not accept object `<obj_name>` in type `<obj_type>`."
    ]
  },
  "CANNOT_ACCESS_TO_DUNDER": {
    "message": [
      "Dunder(double underscore) attribute is for internal use only."
    ]
  },
  "CANNOT_APPLY_IN_FOR_COLUMN": {
    "message": [
      "Cannot apply 'in' operator against a column: please use 'contains' in a string column or 'array_contains' function for an array column."
    ]
  },
  "CANNOT_BE_EMPTY": {
    "message": [
      "At least one <item> must be specified."
    ]
  },
  "CANNOT_BE_NONE": {
    "message": [
      "Argument `<arg_name>` can not be None."
    ]
  },
  "CANNOT_CONVERT_COLUMN_INTO_BOOL": {
    "message": [
      "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions."
    ]
  },
  "CANNOT_CONVERT_TYPE": {
    "message": [
      "Cannot convert <from_type> into <to_type>."
    ]
  },
  "CANNOT_DETERMINE_TYPE": {
    "message": [
      "Some of types cannot be determined after inferring."
    ]
  },
  "CANNOT_GET_BATCH_ID": {
    "message": [
      "Could not get batch id from <obj_name>."
    ]
  },
  "CANNOT_INFER_ARRAY_TYPE": {
    "message": [
      "Can not infer Array Type from an list with None as the first element."
    ]
  },
  "CANNOT_INFER_EMPTY_SCHEMA": {
    "message": [
      "Can not infer schema from empty dataset."
    ]
  },
  "CANNOT_INFER_SCHEMA_FOR_TYPE": {
    "message": [
      "Can not infer schema for type: `<data_type>`."
    ]
  },
  "CANNOT_INFER_TYPE_FOR_FIELD": {
    "message": [
      "Unable to infer the type of the field `<field_name>`."
    ]
  },
  "CANNOT_MERGE_TYPE": {
    "message": [
      "Can not merge type `<data_type1>` and `<data_type2>`."
    ]
  },
  "CANNOT_OPEN_SOCKET": {
    "message": [
      "Can not open socket: <errors>."
    ]
  },
  "CANNOT_PARSE_DATATYPE": {
    "message": [
      "Unable to parse datatype. <msg>."
    ]
  },
  "CANNOT_PROVIDE_METADATA": {
    "message": [
      "metadata can only be provided for a single column."
    ]
  },
  "CANNOT_SET_TOGETHER": {
    "message": [
      "<arg_list> should not be set together."
    ]
  },
  "CANNOT_SPECIFY_RETURN_TYPE_FOR_UDF": {
    "message": [
      "returnType can not be specified when `<arg_name>` is a user-defined function, but got <return_type>."
    ]
  },
  "COLUMN_IN_LIST": {
    "message": [
      "`<func_name>` does not allow a Column in a list."
    ]
  },
  "CONTEXT_ONLY_VALID_ON_DRIVER" : {
    "message" : [
      "It appears that you are attempting to reference SparkContext from a broadcast variable, action, or transformation. SparkContext can only be used on the driver, not in code that it run on workers. For more information, see SPARK-5063."
    ]
  },
  "CONTEXT_UNAVAILABLE_FOR_REMOTE_CLIENT" : {
    "message" : [
      "Remote client cannot create a SparkContext. Create SparkSession instead."
    ]
  },
  "DIFFERENT_PANDAS_DATAFRAME" : {
    "message" : [
      "DataFrames are not almost equal:",
      "Left:",
      "<left>",
      "<left_dtype>",
      "Right:",
      "<right>",
      "<right_dtype>"
    ]
  },
  "DIFFERENT_PANDAS_INDEX" : {
    "message" : [
      "Indices are not almost equal:",
      "Left:",
      "<left>",
      "<left_dtype>",
      "Right:",
      "<right>",
      "<right_dtype>"
    ]
  },
  "DIFFERENT_PANDAS_MULTIINDEX" : {
    "message" : [
      "MultiIndices are not almost equal:",
      "Left:",
      "<left>",
      "<left_dtype>",
      "Right:",
      "<right>",
      "<right_dtype>"
    ]
  },
  "DIFFERENT_PANDAS_SERIES" : {
    "message" : [
      "Series are not almost equal:",
      "Left:",
      "<left>",
      "<left_dtype>",
      "Right:",
      "<right>",
      "<right_dtype>"
    ]
  },
  "DIFFERENT_ROWS" : {
    "message" : [
      "<error_msg>"
    ]
  },
  "DIFFERENT_SCHEMA" : {
    "message" : [
      "Schemas do not match.",
      "--- actual",
      "+++ expected",
      "<error_msg>"
    ]
  },
  "DISALLOWED_TYPE_FOR_CONTAINER" : {
    "message" : [
      "Argument `<arg_name>`(type: <arg_type>) should only contain a type in [<allowed_types>], got <return_type>"
    ]
  },
  "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT" : {
    "message" : [
      "Duplicated field names in Arrow Struct are not allowed, got <field_names>"
    ]
  },
  "EXCEED_RETRY" : {
    "message" : [
      "Retries exceeded but no exception caught."
    ]
  },
  "HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN" : {
    "message" : [
      "Function `<func_name>` should return Column, got <return_type>."
    ]
  },
  "INCORRECT_CONF_FOR_PROFILE" : {
    "message" : [
      "`spark.python.profile` or `spark.python.profile.memory` configuration",
      " must be set to `true` to enable Python profile."
    ]
  },
  "INVALID_ARROW_UDTF_RETURN_TYPE" : {
    "message" : [
      "The return type of the arrow-optimized Python UDTF should be of type 'pandas.DataFrame', but the function returned a value of type <type_name> with value: <value>."
    ]
  },
  "INVALID_BROADCAST_OPERATION": {
    "message": [
      "Broadcast can only be <operation> in driver."
    ]
  },
  "INVALID_CALL_ON_UNRESOLVED_OBJECT": {
    "message": [
      "Invalid call to `<func_name>` on unresolved object."
    ]
  },
  "INVALID_CONNECT_URL" : {
    "message" : [
      "Invalid URL for Spark Connect: <detail>"
    ]
  },
  "INVALID_ITEM_FOR_CONTAINER": {
    "message": [
      "All items in `<arg_name>` should be in <allowed_types>, got <item_type>."
    ]
  },
  "INVALID_NDARRAY_DIMENSION": {
    "message": [
      "NumPy array input should be of <dimensions> dimensions."
    ]
  },
  "INVALID_PANDAS_UDF" : {
    "message" : [
      "Invalid function: <detail>"
    ]
  },
  "INVALID_PANDAS_UDF_TYPE" : {
    "message" : [
      "`<arg_name>` should be one the values from PandasUDFType, got <arg_type>"
    ]
  },
  "INVALID_RETURN_TYPE_FOR_PANDAS_UDF": {
    "message": [
      "Pandas UDF should return StructType for <eval_type>, got <return_type>."
    ]
  },
  "INVALID_TIMEOUT_TIMESTAMP" : {
    "message" : [
      "Timeout timestamp (<timestamp>) cannot be earlier than the current watermark (<watermark>)."
    ]
  },
  "INVALID_TYPE" : {
    "message" : [
      "Argument `<arg_name>` should not be a <data_type>."
    ]
  },
  "INVALID_TYPENAME_CALL" : {
    "message" : [
      "StructField does not have typeName. Use typeName on its type explicitly instead."
    ]
  },
  "INVALID_TYPE_DF_EQUALITY_ARG" : {
    "message" : [
      "Expected type <expected_type> for `<arg_name>` but got type <actual_type>."
    ]
  },
  "INVALID_UDF_EVAL_TYPE" : {
    "message" : [
      "Eval type for UDF must be <eval_type>."
    ]
  },
  "INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE" : {
    "message" : [
      "The UDTF '<name>' is invalid. It has both its return type and an 'analyze' attribute. Please make it have one of either the return type or the 'analyze' static method in '<name>' and try again."
    ]
  },
  "INVALID_UDTF_EVAL_TYPE" : {
    "message" : [
      "The eval type for the UDTF '<name>' is invalid. It must be one of <eval_type>."
    ]
  },
  "INVALID_UDTF_HANDLER_TYPE" : {
    "message" : [
      "The UDTF is invalid. The function handler must be a class, but got '<type>'. Please provide a class as the function handler."
    ]
  },
  "INVALID_UDTF_NO_EVAL" : {
    "message" : [
      "The UDTF '<name>' is invalid. It does not implement the required 'eval' method. Please implement the 'eval' method in '<name>' and try again."
    ]
  },
  "INVALID_UDTF_RETURN_TYPE" : {
    "message" : [
      "The UDTF '<name>' is invalid. It does not specify its return type or implement the required 'analyze' static method. Please specify the return type or implement the 'analyze' static method in '<name>' and try again."
    ]
  },
  "INVALID_WHEN_USAGE": {
    "message": [
      "when() can only be applied on a Column previously generated by when() function, and cannot be applied once otherwise() is applied."
    ]
  },
  "INVALID_WINDOW_BOUND_TYPE" : {
    "message" : [
      "Invalid window bound type: <window_bound_type>."
    ]
  },
  "JAVA_GATEWAY_EXITED" : {
    "message" : [
      "Java gateway process exited before sending its port number."
    ]
  },
  "JVM_ATTRIBUTE_NOT_SUPPORTED" : {
    "message" : [
      "Attribute `<attr_name>` is not supported in Spark Connect as it depends on the JVM. If you need to use this attribute, do not use Spark Connect when creating your session."
    ]
  },
  "KEY_VALUE_PAIR_REQUIRED" : {
    "message" : [
      "Key-value pair or a list of pairs is required."
    ]
  },
  "LENGTH_SHOULD_BE_THE_SAME" : {
    "message" : [
      "<arg1> and <arg2> should be of the same length, got <arg1_length> and <arg2_length>."
    ]
  },
  "MASTER_URL_NOT_SET" : {
    "message" : [
      "A master URL must be set in your configuration."
    ]
  },
  "MISSING_LIBRARY_FOR_PROFILER" : {
    "message" : [
      "Install the 'memory_profiler' library in the cluster to enable memory profiling."
    ]
  },
  "MISSING_VALID_PLAN" : {
    "message" : [
      "Argument to <operator> does not contain a valid plan."
    ]
  },
  "MIXED_TYPE_REPLACEMENT" : {
    "message" : [
      "Mixed type replacements are not supported."
    ]
  },
  "NEGATIVE_VALUE" : {
    "message" : [
      "Value for `<arg_name>` must be greater than or equal to 0, got '<arg_value>'."
    ]
  },
  "NOT_BOOL" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, dict, float, int, str or tuple, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, dict, float, int or str, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_FLOAT_OR_INT" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, float or str, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_FLOAT_OR_INT_OR_LIST_OR_NONE_OR_STR_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, float, int, list, None, str or tuple, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_FLOAT_OR_INT_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a bool, float, int or str, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_LIST" : {
    "message" : [
      "Argument `<arg_name>` should be a bool or list, got <arg_type>."
    ]
  },
  "NOT_BOOL_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a bool or str, got <arg_type>."
    ]
  },
  "NOT_CALLABLE" : {
    "message" : [
      "Argument `<arg_name>` should be a callable, got <arg_type>."
    ]
  },
  "NOT_COLUMN" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_DATATYPE_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, str or DataType, but got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_FLOAT_OR_INT_OR_LIST_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a column, float, integer, list or string, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INT" : {
    "message" : [
      "Argument `<arg_name>` should be a Column or int, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INT_OR_LIST_OR_STR_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, int, list, str or tuple, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_INT_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, int or str, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_LIST_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a Column, list or str, got <arg_type>."
    ]
  },
  "NOT_COLUMN_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a Column or str, got <arg_type>."
    ]
  },
  "NOT_DATAFRAME" : {
    "message" : [
      "Argument `<arg_name>` should be a DataFrame, got <arg_type>."
    ]
  },
  "NOT_DATATYPE_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a DataType or str, got <arg_type>."
    ]
  },
  "NOT_DICT" : {
    "message" : [
      "Argument `<arg_name>` should be a dict, got <arg_type>."
    ]
  },
  "NOT_EXPRESSION" : {
    "message" : [
      "Argument `<arg_name>` should be a Expression, got <arg_type>."
    ]
  },
  "NOT_FLOAT_OR_INT" : {
    "message" : [
      "Argument `<arg_name>` should be a float or int, got <arg_type>."
    ]
  },
  "NOT_FLOAT_OR_INT_OR_LIST_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a float, int, list or str, got <arg_type>."
    ]
  },
  "NOT_IMPLEMENTED" : {
    "message" : [
      "<feature> is not implemented."
    ]
  },
  "NOT_INSTANCE_OF" : {
    "message" : [
      "<value> is not an instance of type <data_type>."
    ]
  },
  "NOT_INT" : {
    "message" : [
      "Argument `<arg_name>` should be an int, got <arg_type>."
    ]
  },
  "NOT_INT_OR_SLICE_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be an int, slice or str, got <arg_type>."
    ]
  },
  "NOT_IN_BARRIER_STAGE" : {
    "message" : [
      "It is not in a barrier stage."
    ]
  },
  "NOT_ITERABLE" : {
    "message" : [
      "<objectName> is not iterable."
    ]
  },
  "NOT_LIST" : {
    "message" : [
      "Argument `<arg_name>` should be a list, got <arg_type>."
    ]
  },
  "NOT_LIST_OF_COLUMN" : {
    "message" : [
      "Argument `<arg_name>` should be a list[Column]."
    ]
  },
  "NOT_LIST_OF_COLUMN_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a list[Column]."
    ]
  },
  "NOT_LIST_OF_FLOAT_OR_INT" : {
    "message" : [
      "Argument `<arg_name>` should be a list[float, int], got <arg_type>."
    ]
  },
  "NOT_LIST_OF_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a list[str], got <arg_type>."
    ]
  },
  "NOT_LIST_OR_NONE_OR_STRUCT" : {
    "message" : [
      "Argument `<arg_name>` should be a list, None or StructType, got <arg_type>."
    ]
  },
  "NOT_LIST_OR_STR_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a list, str or tuple, got <arg_type>."
    ]
  },
  "NOT_LIST_OR_TUPLE" : {
    "message" : [
      "Argument `<arg_name>` should be a list or tuple, got <arg_type>."
    ]
  },
  "NOT_NUMERIC_COLUMNS" : {
    "message" : [
      "Numeric aggregation function can only be applied on numeric columns, got <invalid_columns>."
    ]
  },
  "NOT_OBSERVATION_OR_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a Observation or str, got <arg_type>."
    ]
  },
  "NOT_SAME_TYPE" : {
    "message" : [
      "Argument `<arg_name1>` and `<arg_name2>` should be the same type, got <arg_type1> and <arg_type2>."
    ]
  },
  "NOT_STR" : {
    "message" : [
      "Argument `<arg_name>` should be a str, got <arg_type>."
    ]
  },
  "NOT_STR_OR_LIST_OF_RDD" : {
    "message" : [
      "Argument `<arg_name>` should be a str or list[RDD], got <arg_type>."
    ]
  },
  "NOT_STR_OR_STRUCT" : {
    "message" : [
      "Argument `<arg_name>` should be a str or structType, got <arg_type>."
    ]
  },
  "NOT_WINDOWSPEC" : {
    "message" : [
      "Argument `<arg_name>` should be a WindowSpec, got <arg_type>."
    ]
  },
  "NO_ACTIVE_SESSION" : {
    "message" : [
      "No active Spark session found. Please create a new Spark session before running the code."
    ]
  },
  "ONLY_ALLOWED_FOR_SINGLE_COLUMN" : {
    "message" : [
      "Argument `<arg_name>` can only be provided for a single column."
    ]
  },
  "ONLY_ALLOW_SINGLE_TRIGGER" : {
    "message" : [
      "Only a single trigger is allowed."
    ]
  },
  "PIPE_FUNCTION_EXITED" : {
    "message" : [
      "Pipe function `<func_name>` exited with error code <error_code>."
    ]
  },
  "PYTHON_HASH_SEED_NOT_SET" : {
    "message" : [
      "Randomness of hash of string should be disabled via PYTHONHASHSEED."
    ]
  },
  "PYTHON_VERSION_MISMATCH" : {
    "message" : [
      "Python in worker has different version <worker_version> than that in driver <driver_version>, PySpark cannot run with different minor versions.",
      "Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set."
    ]
  },
  "RDD_TRANSFORM_ONLY_VALID_ON_DRIVER" : {
    "message" : [
      "It appears that you are attempting to broadcast an RDD or reference an RDD from an ",
      "action or transformation. RDD transformations and actions can only be invoked by the ",
      "driver, not inside of other transformations; for example, ",
      "rdd1.map(lambda x: rdd2.values.count() * x) is invalid because the values ",
      "transformation and count action cannot be performed inside of the rdd1.map ",
      "transformation. For more information, see SPARK-5063."
    ]
  },
  "RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF" : {
    "message" : [
      "Column names of the returned pandas.DataFrame do not match specified schema.<missing><extra>"
    ]
  },
  "RESULT_LENGTH_MISMATCH_FOR_PANDAS_UDF" : {
    "message" : [
      "Number of columns of the returned pandas.DataFrame doesn't match specified schema. Expected: <expected> Actual: <actual>"
    ]
  },
  "RESULT_LENGTH_MISMATCH_FOR_SCALAR_ITER_PANDAS_UDF" : {
    "message" : [
      "The length of output in Scalar iterator pandas UDF should be the same with the input's; however, the length of output was <output_length> and the length of input was <input_length>."
    ]
  },
  "SCHEMA_MISMATCH_FOR_PANDAS_UDF" : {
    "message" : [
      "Result vector from pandas_udf was not the required length: expected <expected>, got <actual>."
    ]
  },
  "SESSION_ALREADY_EXIST" : {
    "message" : [
      "Cannot start a remote Spark session because there is a regular Spark session already running."
    ]
  },
  "SESSION_NOT_SAME" : {
    "message" : [
      "Both Datasets must belong to the same SparkSession."
    ]
  },
  "SESSION_OR_CONTEXT_EXISTS" : {
    "message" : [
      "There should not be an existing Spark Session or Spark Context."
    ]
  },
  "SHOULD_NOT_DATAFRAME": {
    "message": [
      "Argument `<arg_name>` should not be a DataFrame."
    ]
  },
  "SLICE_WITH_STEP" : {
    "message" : [
      "Slice with step is not supported."
    ]
  },
  "STATE_NOT_EXISTS" : {
    "message" : [
      "State is either not defined or has already been removed."
    ]
  },
  "STOP_ITERATION_OCCURRED" : {
    "message" : [
      "Caught StopIteration thrown from user's code; failing the task: <exc>"
    ]
  },
  "STOP_ITERATION_OCCURRED_FROM_SCALAR_ITER_PANDAS_UDF" : {
    "message" : [
      "pandas iterator UDF should exhaust the input iterator."
    ]
  },
  "TOO_MANY_VALUES" : {
    "message" : [
      "Expected <expected> values for `<item>`, got <actual>."
    ]
  },
  "UDF_RETURN_TYPE" : {
    "message" : [
      "Return type of the user-defined function should be <expected>, but is <actual>."
    ]
  },
  "UDTF_ARROW_TYPE_CAST_ERROR" : {
    "message" : [
      "Cannot convert the output value of the column '<col_name>' with type '<col_type>' to the specified return type of the column: '<arrow_type>'. Please check if the data types match and try again."
    ]
  },
  "UDTF_EXEC_ERROR" : {
    "message" : [
      "User defined table function encountered an error in the '<method_name>' method: <error>"
    ]
  },
  "UDTF_RETURN_NOT_ITERABLE" : {
    "message" : [
      "The return value of the UDTF is invalid. It should be an iterable (e.g., generator or list), but got '<type>'. Please make sure that the UDTF returns one of these types."
    ]
  },
  "UDTF_RETURN_SCHEMA_MISMATCH" : {
    "message" : [
      "The number of columns in the result does not match the specified schema. Expected column count: <expected>, Actual column count: <actual>. Please make sure the values returned by the function have the same number of columns as specified in the output schema."
    ]
  },
  "UDTF_RETURN_TYPE_MISMATCH" : {
    "message" : [
      "Mismatch in return type for the UDTF '<name>'. Expected a 'StructType', but got '<return_type>'. Please ensure the return type is a correctly formatted StructType."
    ]
  },
  "UDTF_SERIALIZATION_ERROR" : {
    "message" : [
      "Cannot serialize the UDTF '<name>': <message>"
    ]
  },
  "UNEXPECTED_RESPONSE_FROM_SERVER" : {
    "message" : [
      "Unexpected response from iterator server."
    ]
  },
  "UNEXPECTED_TUPLE_WITH_STRUCT" : {
    "message" : [
      "Unexpected tuple <tuple> with StructType."
    ]
  },
  "UNKNOWN_EXPLAIN_MODE" : {
    "message" : [
      "Unknown explain mode: '<explain_mode>'. Accepted explain modes are 'simple', 'extended', 'codegen', 'cost', 'formatted'."
    ]
  },
  "UNKNOWN_INTERRUPT_TYPE" : {
    "message" : [
      "Unknown interrupt type: '<interrupt_type>'. Accepted interrupt types are 'all'."
    ]
  },
  "UNKNOWN_RESPONSE" : {
    "message" : [
      "Unknown response: <response>."
    ]
  },
  "UNSUPPORTED_DATA_TYPE" : {
    "message" : [
      "Unsupported DataType `<data_type>`."
    ]
  },
  "UNSUPPORTED_DATA_TYPE_FOR_ARROW" : {
    "message" : [
      "Single data type <data_type> is not supported with Arrow."
    ]
  },
  "UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION" : {
    "message" : [
      "<data_type> is not supported in conversion to Arrow."
    ]
  },
  "UNSUPPORTED_DATA_TYPE_FOR_ARROW_VERSION" : {
    "message" : [
      "<data_type> is only supported with pyarrow 2.0.0 and above."
    ]
  },
  "UNSUPPORTED_JOIN_TYPE" : {
    "message" : [
      "Unsupported join type: <join_type>. Supported join types include: \\"inner\\", \\"outer\\", \\"full\\", \\"fullouter\\", \\"full_outer\\", \\"leftouter\\", \\"left\\", \\"left_outer\\", \\"rightouter\\", \\"right\\", \\"right_outer\\", \\"leftsemi\\", \\"left_semi\\", \\"semi\\", \\"leftanti\\", \\"left_anti\\", \\"anti\\", \\"cross\\"."
    ]
  },
  "UNSUPPORTED_LITERAL" : {
    "message" : [
      "Unsupported Literal '<literal>'."
    ]
  },
  "UNSUPPORTED_NUMPY_ARRAY_SCALAR" : {
    "message" : [
      "The type of array scalar '<dtype>' is not supported."
    ]
  },
  "UNSUPPORTED_OPERATION" : {
    "message" : [
      "<operation> is not supported."
    ]
  },
  "UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION" : {
    "message" : [
      "Function `<func_name>` should use only POSITIONAL or POSITIONAL OR KEYWORD arguments."
    ]
  },
  "UNSUPPORTED_SIGNATURE" : {
    "message" : [
      "Unsupported signature: <signature>."
    ]
  },
  "UNSUPPORTED_WITH_ARROW_OPTIMIZATION" : {
    "message" : [
      "<feature> is not supported with Arrow optimization enabled in Python UDFs. Disable 'spark.sql.execution.pythonUDF.arrow.enabled' to workaround.."
    ]
  },
  "VALUE_NOT_ACCESSIBLE": {
    "message": [
      "Value `<value>` cannot be accessed inside tasks."
    ]
  },
  "VALUE_NOT_ANY_OR_ALL" : {
    "message" : [
      "Value for `<arg_name>` must be 'any' or 'all', got '<arg_value>'."
    ]
  },
  "VALUE_NOT_BETWEEN" : {
    "message" : [
      "Value for `<arg_name>` must be between <min> and <max>."
    ]
  },
  "VALUE_NOT_NON_EMPTY_STR" : {
    "message" : [
      "Value for `<arg_name>` must be a non empty string, got '<arg_value>'."
    ]
  },
  "VALUE_NOT_PEARSON" : {
    "message" : [
      "Value for `<arg_name>` only supports the 'pearson', got '<arg_value>'."
    ]
  },
  "VALUE_NOT_POSITIVE" : {
    "message" : [
      "Value for `<arg_name>` must be positive, got '<arg_value>'."
    ]
  },
  "VALUE_NOT_TRUE" : {
    "message" : [
      "Value for `<arg_name>` must be True, got '<arg_value>'."
    ]
  },
  "VALUE_OUT_OF_BOUND" : {
    "message" : [
      "Value for `<arg_name>` must be greater than <lower_bound> or less than <upper_bound>, got <actual>"
    ]
  },
  "WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION" : {
    "message" : [
      "Function `<func_name>` should take between 1 and 3 arguments, but provided function takes <num_args>."
    ]
  },
  "WRONG_NUM_COLUMNS" : {
    "message" : [
      "Function `<func_name>` should take at least <num_cols> columns."
    ]
  }
}
"""

ERROR_CLASSES_MAP = json.loads(ERROR_CLASSES_JSON)
