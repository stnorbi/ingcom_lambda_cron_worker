import json, os, copy
import boto3
import pandas as pd


def ingcom_submit(event, context):

    # ensure list of search parameters (city, property type, deal type) are present in request
    if 'city_unic' not in event or 'property_type' not in event or 'deal' not in event:
        PROPERTY_TYPE='+'
        DEAL='+'
        return json.dumps({
            'message': 'No search parameters supplied'
        })
    else:
        CITY = event['city_unic']
        PROPERTY_TYPE = event['property_type']
        DEAL=event['deal']
        
    # create boto3 clients for s3 and batch
    batch = boto3.client('batch', region_name='eu-central-1')
    s3 = boto3.client('s3', region_name='eu-central-1')
    


    # create dictionary of job definition
    job_definition = {
        'jobName': 'ingatlancom-scraper',
        'jobDefinition': 'house-scraper:'+os.environ['JOB_VERSION'],
        'jobQueue': 'ingcom-scraper',
        'containerOverrides': {
            'environment': [
                {
                    'name': 'S3_BUCKET',
                    'value': 'all-types-ingcom-data'
                },
                {
                    'name': 'ENV',
                    'value': 'production'
                },
                {
                    'name': 'CONCURRENT_REQUESTS',
                    'value': os.environ['CONCURRENT_REQUESTS']
                },
                {
                    'name': 'CONCURRENT_REQUESTS_PER_DOMAIN',
                    'value': os.environ['CONCURRENT_REQUESTS_PER_DOMAIN']
                },
                {
                    'name': 'CONCURRENT_REQUESTS_PER_IP',
                    'value': os.environ['CONCURRENT_REQUESTS_PER_IP']
                },                      
                {
                    'name': 'CONCURRENT_ITEMS',
                    'value': os.environ['CONCURRENT_ITEMS']
                },
                {
                    'name': 'DOWNLOAD_DELAY',
                    'value': os.environ['DOWNLOAD_DELAY']
                },
                {
                    'name': 'RANDOMIZE_DOWNLOAD_DELAY',
                    'value': os.environ['RANDOMIZE_DOWNLOAD_DELAY']
                },
                {
                    'name': 'REDIRECT_ENABLED',
                    'value': os.environ['REDIRECT_ENABLED']
                },
                {
                    'name': 'REACTOR_THREADPOOL_MAXSIZE',
                    'value': os.environ['REACTOR_THREADPOOL_MAXSIZE']
                },
                {
                    'name': 'DOWNLOAD_TIMEOUT',
                    'value': os.environ['DOWNLOAD_TIMEOUT']
                },
                {
                    'name': 'TELNETCONSOLE_ENABLED',
                    'value': os.environ['TELNETCONSOLE_ENABLED']
                },
                {
                    'name': 'AUTOTHROTTLE_ENABLED',
                    'value': os.environ['AUTOTHROTTLE_ENABLED']
                },
                {
                    'name': 'AUTOTHROTTLE_START_DELAY',
                    'value': os.environ['AUTOTHROTTLE_START_DELAY']
                },
                {
                    'name': 'AUTOTHROTTLE_MAX_DELAY',
                    'value': os.environ['AUTOTHROTTLE_MAX_DELAY']
                },
                {
                    'name': 'TELNETCONSOLE_ENABLED',
                    'value': os.environ['TELNETCONSOLE_ENABLED']
                },
                {
                    'name': 'AUTOTHROTTLE_TARGET_CONCURRENCY',
                    'value': os.environ['AUTOTHROTTLE_TARGET_CONCURRENCY']
                },
                {
                    'name': 'AUTOTHROTTLE_DEBUG',
                    'value': os.environ['AUTOTHROTTLE_DEBUG']
                },
                {
                    'name': 'RETRY_ENABLED',
                    'value': os.environ['RETRY_ENABLED']
                },
                {
                    'name': 'RETRY_HTTP_CODES',
                    'value': os.environ['RETRY_HTTP_CODES']
                },
                {
                    'name': 'LOG_LEVEL',
                    'value': os.environ['LOG_LEVEL']
                }
            ]
        }
    }
    
    # get list of cities from s3 csv
    bucketName = 'all-types-ingcom-data'
    cityCsvName = 'ingcom_db/ingcom_cities.csv'
    city_obj = s3.get_object(Bucket=bucketName, Key=cityCsvName)
    df = pd.read_csv(city_obj['Body'])

    searches = df[df['city_unic'].isin(CITY)]
    
    # placeholder for jobs to be submitted
    job_queue = []
    # create jobs to be submitted
    
    for deal in DEAL:
        for property_type in PROPERTY_TYPE:
            for i, city in searches.iterrows():
                _job = copy.deepcopy(job_definition)

                _job['containerOverrides']['environment'].append({
                'name': 'SEARCH_CITY_URL',
                'value': city['link']
                }),
                _job['containerOverrides']['environment'].append({
                'name': 'DEAL',
                'value': deal
                }),
                _job['containerOverrides']['environment'].append({
                'name': 'PROPERTY_TYPE',
                'value': property_type
                }),
                _job['containerOverrides']['environment'].append({
                'name': 'CITY',
                'value': city['city_unic']
                })
                _city = ''.join(s for s in city['city_unic'] if s.isalnum())
                _job['jobName'] = f'{property_type}-{_city}-{deal}'
                job_queue.append(_job)
    

    for j in job_queue:
        res = batch.submit_job(**j)
        print('SUBMITTED:', res['jobName'], 'JOB_ID:', res['jobId'])
    return json.dumps({
        'message': f'submitted {len(job_queue)} jobs'
    })



