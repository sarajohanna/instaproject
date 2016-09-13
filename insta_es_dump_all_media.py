import time
import argparse
import requests
import elasticsearch

# NOQA Load the secrets from file so some scrublord like me doesn't accidentally commit them to git.

SEND_TO_ES = True

secrets = argparse.Namespace()
with open("SECRETS.txt") as f:
    for line in f:
        vals = line.strip().split('=', 1)
        setattr(secrets, vals[0].lower(), vals[1])

'''
Make sure you PUT the following to init the index 'tweeter_dump'
with mapping for the 'tweet' doc type to your cluster first.

Otherwise the @timestamp will be a float and not an epoch_millis.

PUT /instagram_dump_sarali/

{
    "mappings": {
        "like": {
            "properties": {
                "post": {
                    "type": "string"
                },
                "text": {
                    "type": "string"
                },
                "url": {
                    "type": "string"
                },
                "media_type": {
                    "type": "string"
                },
                "username": {
                    "type": "string"
                },
                "@timestamp": {
                    "format": "epoch_millis",
                    "type": "date"
                }
            }
        }
    }
}
'''

auth = secrets.insta_access_token


es = elasticsearch.Elasticsearch([
    "{}//{}:{}@{}".format(
        secrets.es_url.split('//', 1)[0],
        secrets.es_user,
        secrets.es_pass,
        secrets.es_url.split('//', 1)[1])])
print 'sending instagram data to ', secrets.es_url

# getting the ids from my recent insta posts
def get_insta_post_ids():
    # getting my recent posts from instagram (20 latest)
    insta_adress = "https://api.instagram.com/v1/users/self/media/recent?access_token={}".format(auth)
    r = requests.get(insta_adress)
    insta_post = r.json()
    print "\n\n\n\n\n\n\n"
    print insta_post
    raise

    # getting the user data for each post
    for media in insta_post['data']:
        media_id = media['id']
        media_text = media['caption']['text']
        media_url = media['link']
        timestamp = media['created_time']
        media_type = media['type']
        insta_media_adress = "https://api.instagram.com/v1/media/{}/likes?access_token={}".format(media_id, auth)
        p = requests.get(insta_media_adress)
        insta_likes = p.json()
        print media_id

        # getting the user inside the user data dictionaries
        for user_info in insta_likes['data']:
            username = user_info['username']
            doc = {
                'post': media_id,
                'text': media_text,
                'url': media_url,
                'media_type': media_type,
                'username': username,
                '@timestamp': int(int(timestamp) * 1000)
            }
            # print "\n\n\n\n\n\n\n"
            # print doc
            es.create(
                index='instagram_dump_sarali',
                doc_type='like',
                body=doc)
            # print "successfully put to ES!"

get_insta_post_ids()



# def on_error(self, status_code):
#     if status_code == 420:
#         # returning False in on_data disconnects the stream
#         return False
