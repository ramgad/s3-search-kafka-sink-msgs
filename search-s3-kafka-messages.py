import boto3
import os
import logging
import time
import pprint
import concurrent.futures
import configparser
from multiprocessing import Process
from datetime import datetime, timedelta

# Read configuration from the file
config = configparser.ConfigParser()
config.read('config.ini')


# Configuration parameters
config_search_criteria = config.get('General', 'search_criteria', fallback=["10192329 WGTK TR", "10013624 CLR", "10099145 LIN"]).split(', ')
config_search_date = config.get('General', 'search_date', fallback=(datetime.now() - timedelta(days=2)).strftime("%Y/%m/%d")).split("/")
config_search_hour = int(config.get('General', 'search_hour', fallback=6))
config_search_topics = config.get('General', 'search_topics', fallback="queuing.inventory.skuNumber").split(', ')
config_threads = int(config.get('General', 'threads', fallback=64))
bucket_name = config.get('General', 'bucket_name', fallback="rh-kafka-firehose-s3-triage")
print_matched_content = config.getboolean('General', 'print_matched_content', fallback=True)
config_log_level = config.get('General', 'log_level', fallback='INFO')


# Configure the logging module
log_folder = "logs"  # Specify the folder for log files
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_filename = f"{log_folder}/search_s3_kafka_messages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_filename, level=config_log_level, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


#bucket_name         = "rh-kafka-firehose-s3-triage"
s3MatchFoundKeys    = []
TotalObjectsScanned = 0
keys_all_pages      = []
matches             = {}

# Function to get yesterday's date
def get_yesterday():
    return (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")


def find_match_s3_key(inputKey, search_hour, search_criteria):
    logger.debug(f"find_match_s3_key: config_search_criteria: {config_search_criteria}, config_search_date : {config_search_date}, config_search_hour: {config_search_hour}, config_search_topics: {config_search_topics}, config_bucket_name: {bucket_name}, config_print_matched_content : {print_matched_content} ")
    global TotalObjectsScanned  
    global s3MatchFoundKeys
    global matches
    matchFound = False
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        #prefix_query=f"topics/{search_topic}/year={search_date[0]}/month={search_date[1]}/day={search_date[2]}/hour={str(search_hour).zfill(2)}/"
        response = s3.get_object(Bucket=bucket_name, Key=inputKey)
    except Exception as e:
        logger.error(f"find_match_s3_key: Error processing ObjectKey: {e}")
        print(f"find_match_s3_key: Error processing ObjectKey: {e}")
    with response['Body'] as s3_object:
        object_content = s3_object.read().decode('utf-8')
        logger.debug(object_content)
        # Check if the search string is present in the XML content
        TotalObjectsScanned += 1
        # Check if TotalObjectsScanned is a multiple of 1000
        if TotalObjectsScanned % 1000 == 0:
            logger.info(f'TotalObjectsScanned so far: {TotalObjectsScanned}')
            print(f'TotalObjectsScanned so far: {TotalObjectsScanned}')
        for query in search_criteria:
            logger.debug(f"find_match_s3_key: query for the match : {query}")
            #matches[query].append(file_key for file_key, content in matches.items() if query in object_content)
            if query in object_content:
                logger.debug(f"find_match_s3_key match found before adding to the dictionary: -> Query: {query}, Hour: {search_hour}, inputKey : {inputKey}")
                if query not in matches:
                    matches[query]=[]
                matches[query].append(inputKey)
                s3MatchFoundKeys.append(inputKey)
                print(f"find_match_s3_key match found:  s3 key : {inputKey}")
                logger.info(f"find_match_s3_key match found:  s3 key : {inputKey}")
                if print_matched_content:
                    logger.info("S3 Object Content: %s", object_content)
                    print("S3 Object Content: %s", object_content)
                logger.debug(f"find_match_s3_key yes match found: -> Hour: {search_hour} : {inputKey}")
                matchFound = True

    logger.debug(f"find_match_s3_key: No match found {inputKey} : Hour - {search_hour}")
    if matchFound:
        logger.debug("Match Found --> true")
        return inputKey
    else:
        return []


def get_all_keys(objects, pageNo):
    logger.debug(f"get_all_keys: config_search_criteria: {config_search_criteria}, config_search_date : {config_search_date}, config_search_hour: {config_search_hour}, config_search_topics: {config_search_topics}, config_bucket_name: {bucket_name}, config_print_matched_content : {print_matched_content} ")
    print(f"get_all_keys: invoked -> page: {pageNo}")
    global keys_all_pages
    for obj in objects:
        keys_all_pages.append(obj["Key"])


def search_s3_bucket_by_day(search_topic, search_criteria, search_date, search_hour):
    logger.debug(f"search_s3_bucket_by_day: config_search_criteria: {config_search_criteria}, config_search_date : {config_search_date}, config_search_hour: {config_search_hour}, config_search_topics: {config_search_topics}, config_bucket_name: {bucket_name}, config_print_matched_content : {print_matched_content} ")
    global TotalObjectsScanned  # Declare the variable as global
    global s3MatchFoundKeys
    global keys_all_pages
    returnkeys_byhour=[]
    list_of_tokens = []
    threads = []
    pageNo=1
    logger.debug(f"search_s3_bucket_by_day -> search_topic: {search_topic} AND search_hour :{search_hour}")

    if not int(search_hour) <25:
        #raise error("Input Search Hour is not below 25")
        return ""

    # if (int(search_by_hour) != int(search_hour)):
    #     logger.debug(f"search_s3_bucket_by_day search_by_hour:{search_by_hour} and search_hour is {search_hour}")
    #     logger.debug(f"search_s3_bucket_by_day: Returning back -> hour: {search_by_hour}")
    #     return ""
    # logger.debug(f"search_s3_bucket_by_day SEARCHING ----> search_by_hour:{search_by_hour} and search_hour is {search_hour}")
    print(f"SEARCHING ----> topic: {search_topic}, and for hour: {str(search_hour).zfill(2)}")

    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        prefix_query=f"topics/{search_topic}/year={search_date[0]}/month={search_date[1]}/day={search_date[2]}/hour={str(search_hour).zfill(2)}/"
        logger.debug(f"search_s3_bucket_by_day: {prefix_query}")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix_query}")
        logger.debug(f"search_s3_bucket_by_day: Initial count from search_s3_bucket_by_day: {len(response['Contents'])}")
    except Exception as e:
        logger.error(f"search_s3_bucket_by_day: Error processing s3 boto call: {e}")
        print(f"search_s3_bucket_by_day: Error processing s3 boto call: {e}")

    if response.get("Contents"):
        get_all_keys(response["Contents"], pageNo)
    else:
        print("WARN: search_s3_bucket_by_day: topic: {search_topic} and hour: {search_hour} - has no s3 content available")
        logger.warn("search_s3_bucket_by_day: topic: {search_topic} and hour: {search_hour} - has no s3 content available")
        return ""

    # Continue paginating if there are more objects
    while response.get('IsTruncated', False):
        if 'NextContinuationToken' in response:
            pageNo+=1
            #print(f"search_s3_bucket_by_day: Pulling Keys for Page: {pageNo} for Hour: {str(search_by_hour).zfill(2)} ")
            #list_of_tokens.append(response['NextContinuationToken'])
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix_query}", ContinuationToken=response['NextContinuationToken'])
            get_all_keys(response["Contents"], pageNo)
    logger.info(f"search_s3_bucket_by_day: Pulling Keys per hour: {search_hour} completed -> Keys found: {len(keys_all_pages)}")
    print(f"search_s3_bucket_by_day: Pulling Keys per hour: {search_hour} completed -> Keys found: {len(keys_all_pages)}")
    l_tasks = [] 

    #Loop through the objects for scanning using multi-threading
    with concurrent.futures.ThreadPoolExecutor(config_threads) as executor2:
        logger.debug(f"search_s3_bucket_by_day: Processing objects for the given keys: {str(search_hour).zfill(2)}")
        # Submit tasks and get Future objects
        for objectKey in keys_all_pages:
            logger.debug(f"search_s3_bucket_by_day: Thread -> Hour: {search_hour} - PageNo: {pageNo} -> Invoking a a new thread with objectKey is {objectKey}")
            #response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix_query}", ContinuationToken=Token) 
            logger.debug(f"l_tasks.append(executor2.submit(find_match_s3_key, {objectKey}, {search_hour}))")
            l_tasks.append(executor2.submit(find_match_s3_key, objectKey, search_hour, search_criteria))
        
        # Wait for the tasks to complete
        for task in concurrent.futures.as_completed(l_tasks):
            try:
                logger.debug(f"Hour: {search_hour} - Retrieving the results from the thread")
                logger.debug(f"Results : {task.result()}")
                if len(task.result()) > 0:
                    logger.debug(f"search_s3_bucket_by_day: Match Found with the key : {task.result()}")
                    returnkeys_byhour.append(task.result())
            except Exception as e:
                logger.error(f"search_s3_bucket_by_day: Error processing ObjectKey: {e}")
                print(f"search_s3_bucket_by_day: topic: {search_topic}, Error processing ObjectKey: {e}")
    
    logger.debug(list_of_tokens)
    logger.info(f"search_s3_bucket_by_day: Completed running for topic: {search_topic}, hour: {str(search_hour).zfill(2)} Returning: {len(returnkeys_byhour)}")
    print(f"search_s3_bucket_by_day: Completed running for topic: {search_topic}, hour: {str(search_hour).zfill(2)} Returning: {len(returnkeys_byhour)}")
    results = [item for sublist in returnkeys_byhour if sublist for item in sublist]
    return results

def main(search_criteria, search_date, search_hour, topics):
    logger.debug(f"main: config_search_criteria: {config_search_criteria}, config_search_date : {config_search_date}, config_search_hour: {config_search_hour}, config_search_topics: {config_search_topics}, config_bucket_name: {bucket_name}, config_print_matched_content : {print_matched_content} ")
    global matches
    t1 = time.perf_counter()
    returnKeys=[]
    procs = []

    logger.info(f"__main__:  search_criteria: {search_criteria}")
    logger.info(f"__main__:  search_topics: {topics}")
    print("***************************************************************")
    print(f"__main__:  search_criteria: {search_criteria}")
    print(f"__main__:  search_topics: {topics}")
    print(f"__main__:  search_date: {search_date}")
    print(f"__main__:  search_hour: {search_hour}")
    print("***************************************************************")

    # instantiating process with arguments
    for topic in topics:
        logger.info (f"__main__: topic is -> {topic}")
        proc = Process(target=main_multi_processor, args=(topic, search_criteria, search_date, search_hour, ))
        procs.append(proc)
        proc.start()
        print(f"__main__:  Invoking multiprocessing for topic: {topic}")

    # complete the processes
    for proc in procs:
        proc.join()
        print(f"__main__:  Completed multiprocessing for topic: {topic}")


    t2 = time.perf_counter()
    elapsed_time_seconds = t2 - t1
    elapsed_time_minutes = int(elapsed_time_seconds // 60)
    remaining_seconds = int(elapsed_time_seconds % 60)

    print('')
    print(f'__main__: Finished Program - {elapsed_time_minutes} minutes and {remaining_seconds} seconds')
    print("***************************************************************")
    print('')
    logger.info('')
    logger.info(f'__main__: Finished Program - {elapsed_time_minutes} minutes and {remaining_seconds} seconds')
    logger.info("***************************************************************")
    logger.info('')



def main_multi_processor(search_topic, search_criteria, search_date, search_hour):
    logger.debug(f"main_multi_processor: config_search_criteria: {config_search_criteria}, config_search_date : {config_search_date}, config_search_hour: {config_search_hour}, config_search_topics: {config_search_topics}, config_bucket_name: {bucket_name}, config_print_matched_content : {print_matched_content} ")
    global TotalObjectsScanned  # Declare the variable as global
    global s3MatchFoundKeys
    global keys_all_pages
    t1 = time.perf_counter()
    # Number of threads
    num_threads = 24
    # Using ThreadPoolExecutor to run tasks in parallel
    returnKeys=[]
    logger.info("******************** Inside main method ******************")
    logger.info(f"main_multi_processor - args: {search_topic}")

    results = search_s3_bucket_by_day(search_topic, search_criteria, search_date, search_hour)
    returnKeys.extend(results)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    #     # Start threads for tasks
    #     futures0 = [executor.submit(search_s3_bucket_by_day, search_topic, i, search_criteria, search_date, search_hour) for i in range(num_threads)]
    #     # Wait for all threads to complete and get results
    #     try:
    #         results = [future0.result() for future0 in concurrent.futures.as_completed(futures0)]
    #         returnKeys.extend(results)
    #     except Exception as e:
    #         logger.error(f"main_multi_processor: Error processing in main multithreading: {e}")
    #         print(f"main_multi_processor: Error processing in main multithreading: {e}")
    # results = [item for sublist in returnKeys if sublist for item in sublist]
    logger.info("")
    t2 = time.perf_counter()
    logger.info(f'main_multi_processor: Finished in {t2-t1} seconds')
    logger.info("")
    logger.info(f"main_multi_processor: Total number of S3 objects scanned : {TotalObjectsScanned}")
    if len(matches) > 0:
        logger.info("main_multi_processor: Printing pprint of matches dictionary")
        logger.info(matches)
        pprint.pprint(matches)
        print("")
        print(f"Found the \"{search_criteria}\" in the following s3 xml files for the topic: \"{search_topic}\"")
        print(f"You can download the content of the files using the following s3 copy commands.. ")
        print(f"----------------------------------------------------------------------")

        logger.info("")
        logger.info(f"Found the \"{search_criteria}\" in the following s3 xml files for the topic: \"{search_topic}\"")
        logger.info(f"You can download the content of the files using the following s3 copy commands.. ")
        logger.info(f"----------------------------------------------------------------------")
        for i in s3MatchFoundKeys:
            logger.info(f"aws s3 cp s3://{bucket_name}/{i} .")
            print(f"aws s3 cp s3://{bucket_name}/{i} .")
        logger.info(f"----------------------------------------------------------------------")
    else:
        print(f"Did not find the \"{search_criteria}\" for the given day {search_date} in the \"{search_topic}\"")
        logger.info(f"Did not find the \"{search_criteria}\" for the given day {search_date} in the \"{search_topic}\"")
        logger.info(f"----------------------------------------------------------------------")

if __name__ == "__main__":
    #global keys_all_pages
    # Prompt the user to enter values
    # search_criteria  = input( "Enter Search Criteria (default ['10192329 WGTK TR', '10013624 CLR', '10099145 LIN']) : ") or ["10192329 WGTK TR", "10013624 CLR", "10099145 LIN"]
    # search_date      = input(f"Enter Search Date - yyyy/mm/dd (default: {get_yesterday()})            : ") or config_search_date
    # search_hour      = int(input(f"Enter Search Hour (default: 'OO' 24HH format): ").strip() or config_search_hour)
    # topics           = input(f"Enter Topics (default ['queuing.inventory.skuNumber'] : ") or config_search_topics

    search_criteria = config_search_criteria
    search_date     = config_search_date
    search_hour     = config_search_hour
    topics          = config_search_topics

    # Display the entered values
    logger.info("\nConfig Criteria values:")
    logger.info("Search Criteria: %s", search_criteria)
    logger.info("Search Date: %s", search_date)
    logger.info("Search Hour (24 HH): %s", search_hour)
    logger.info("topics : %s", topics) 

    logger.info("")
    #search_date = search_date.split("/")

    main(search_criteria, search_date, search_hour, topics)


###
# Author - Ramesh Gadamsetti
# Date   - Jan 5th 2024 (v1)
#  
# For questions or suggestions, please slack on #rhapsody-support channel
#
# TODOs - Nice to Haves
#   Convert to have multiple classes
#   Utilize dq - data queue data structure & pop method
#   Conver to Fascade pattern
#   Productionalize 
#       Have test classes with mock
#       Have aws credentials auto-renew or usage IAM
#       Run in K8s with IAM
#
#
####


