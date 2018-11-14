import requests
import json
import time
import boto3


def get_tags(page=1, in_lambda=False, topic_arn=''):
    print("get_tags:page="+str(page)+":in_lambda="+str(in_lambda)+":topic_arn="+topic_arn) 
    print("get_tags:In Lambda?:" + str(in_lambda)) 

    max_time = 60*4;  # At 4 minutes, send an SNS message to continue and return from this function
    start_time = time.time()

    site = "stackoverflow"

    url_prototype = "https://api.stackexchange.com/2.2/tags?&page={0}&order=desc&sort=popular&site={1}&pagesize=100&key=AMvC9mBZAOmMiqz41c5vfg(("

    today = time.strftime("%m/%d/%Y")

    return_val = ""
    returned_item_count = 0
    has_more = True

    while has_more:

        request_str = url_prototype.format(page, site)
        print("request:"+request_str)
        response = requests.get(request_str)

        data = json.loads(response.text)

        for item in data["items"]:
            output = "{0}:{1}:{2}:{3}".format(today, site, item["name"], item["count"])
            if not in_lambda:
                print output
            return_val = return_val + output + '\n'
            returned_item_count += 1

        if 'backoff' in data:
            backoff_time = int(data['backoff']) + 5
            print ("Backoff:" + str(backoff_time))
            time.sleep(backoff_time)
            print("resuming")

        has_more = data["has_more"]
        print("has_more:"+str(has_more))
        print("in_lambda:"+str(in_lambda))

        page = int(page) + 1
        print("next page="+str(page))

        print (time.time()-start_time)
        if ((time.time()-start_time) >= max_time) & (has_more):
            if in_lambda:
                client = boto3.client('sns')
                response = client.publish(
                    TargetArn=topic_arn,
                    Message=str(page)
                )
                print("Publish SNS trigger, page=" + str(page))
                break
            else:
                return_val += get_tags(page,in_lambda,topic_arn)
                break

    return return_val



def lambda_handler(event, context):
    print("In Lambda")
    
    print("Received event: " + json.dumps(event, indent=2))
    if 'Records' in event:
        page = event['Records'][0]['Sns']['Message']
        print("From SNS - page: " + page)
        topic_arn = event['Records'][0]['Sns']['TopicArn']
        print("TopicArn:" + topic_arn)
        results = get_tags(page, True, topic_arn)
        return results
    else:
        print("No SNS message received")
        results = get_tags(1,True)
        return results

get_tags()

