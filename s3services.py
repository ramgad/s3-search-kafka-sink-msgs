# Subsystem classes
class s3services:
    def __init__(self) -> None:
        pass

    def find_match_s3_key(self, inputKey, search_hour, search_criteria):
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