import json, os, copy
import boto3
import pandas as pd


def ingcom_submit(event, context):

    # ensure list of search parameters (states, makes) are present in request
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
        'jobDefinition': 'house-scraper:10',
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
                }
            ]
        }
    }
    
    # get list of cities from s3 csv
    bucketName = 'all-types-ingcom-data'
    cityCsvName = 'ingcom_db/ingcom_cities.csv'
    city_obj = s3.get_object(Bucket=bucketName, Key=cityCsvName)
    df = pd.read_csv(city_obj['Body'])
    # df=pd.read_csv("/mnt/A42A8BE22A8BB03A/Python/AWS_IC_Scraper/ingcom_scraper/db/cities/ingcom_cities.csv")
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
    
    # for i in job_queue:
    #     print(i.items())

    for j in job_queue:
        res = batch.submit_job(**j)
        print('SUBMITTED:', res['jobName'], 'JOB_ID:', res['jobId'])
    return json.dumps({
        'message': f'submitted {len(job_queue)} jobs'
    })



