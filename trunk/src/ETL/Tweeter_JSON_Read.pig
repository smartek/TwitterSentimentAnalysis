--REGISTER '/home/venkatramanann/json-simple-read-only/target/json-simple-1.1.1.jar';
REGISTER '$PIGPATH/build/ivy/lib/Pig/guava-11.0.jar';
REGISTER '$PIGPATH/lib/elephant-bird-core-3.0.6.jar';
REGISTER '$PIGPATH/lib/elephant-bird-pig-3.0.6.jar';
REGISTER '$PIGPATH/build/ivy/lib/Pig/log4j-1.2.16.jar';
REGISTER '/home/venkatramanann/Downloads/google-collect-snapshot-20080530.jar';
REGISTER '/home/venkatramanann/Downloads/slf4j-1.7.2/slf4j-log4j12-1.7.2.jar';
REGISTER '/home/venkatramanann/Downloads/slf4j-1.7.2/slf4j-log4j12-1.7.2-sources.jar';
REGISTER '/home/venkatramanann/Downloads/commons-lang-2.5.jar';
REGISTER '/home/venkatramanann/Downloads/json-smart-1.0.6.3.jar';
REGISTER '/home/venkatramanann/Downloads/pig-0.10.1/build/ivy/lib/Pig/json-simple-1.1.jar';

LoadRawJSON = LOAD '$JSONPATH' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (json:map[]);
Results = FOREACH LoadRawJSON GENERATE FLATTEN(json#'results') AS results;

Text = FOREACH Results GENERATE FLATTEN(results#'from_user_id'), FLATTEN(results#'text'), FLATTEN(results#'from_user_id_str'), FLATTEN(results#'from_user'), FLATTEN(results#'geo'), FLATTEN(results#'id'), FLATTEN(results#'iso_language_code'),FLATTEN(results#'to_user_id'),FLATTEN(results#'to_user_id_str'), FLATTEN(results#'created_at');

STORE Text INTO '/home/hduser/TwetterData1';




