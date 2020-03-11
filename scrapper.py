from collections import defaultdict
from tqdm import tqdm
import json
import os
import random
import pandas as pd
from psaw import PushshiftAPI

"""# List of subreddits and bots"""

technical_subreddits = [
            'askscience',
            'bitcoin',
            'legaladvice',
            'machinelearning',
            'math',
            'science'
          ]
generic_subreddits = [ 
            'askreddit',
            'jokes',
            'tifu',
            'showerthoughts',
            'todayilearned',
            'quotes'
          ]

"""# Scrapping subreddits

Here I will start scraping subreddits. The steps involved are:
- First, get all submissions from the subreddit
- Iterate over each submission and get all the comments from that submission
- It is probably a good idea to store the last submission that I retrieve, so that I can then start scraping from that exact same position later on
"""

def get_subreddit_posts(api, subreddit, before=None, after=None, limit=None):
    return api.search_submissions(after=after, before=before, subreddit=subreddit, limit=limit)

def get_post_comments(api, link_id):
    return api.search_comments(link_id=link_id)

api = PushshiftAPI()
last_post_time = defaultdict(lambda:0)
posts_per_iteration = 10000
subreddits = technical_subreddits + generic_subreddits
random.shuffle(subreddits)
destination_folder = "output/"
iterations = 0
while(True):
    for subreddit in subreddits:
        df_posts = pd.DataFrame(columns=['id', 'created_utc', 'url', 'title', 'text'])
        df_comments = pd.DataFrame(columns=['id', 'parent_id', 'created_utc', 'body'])
        before = None if last_post_time[subreddit] == 0 else last_post_time[subreddit]
        submissions = list(get_subreddit_posts(api,subreddit, limit=posts_per_iteration, before=before))
        pbar = tqdm(submissions)
        pbar.set_description(f"Iter: {iterations} Processing: {subreddit}")
        for submission in pbar:
            try:
                if submission.author != '[deleted]' or submission.author != '[removed]':
                    if last_post_time[subreddit] == 0 or last_post_time[subreddit] > submission.created_utc:
                        last_post_time[subreddit] = submission.created_utc
                    df_posts = df_posts.append({'id':submission.id, 'created_utc':submission.created_utc, 'url':submission.url, 'title':submission.title, 'text':submission.selftext}, ignore_index=True)
                    comments = list(get_post_comments(api, submission.id))
                    for comment in comments:
                        if comment.body != '[deleted]' or comment.body != '[removed]':
                            df_comments = df_comments.append({'id':comment.id, 'parent_id':comment.parent_id, 'created_utc':comment.created_utc, 'body':comment.body}, ignore_index=True)
            except Exception as e:
                print (f"Got an error: {e}")
        output_dir = f"{destination_folder}{subreddit}/{last_post_time[subreddit]}/"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        df_posts.to_csv(f"{output_dir}{subreddit}_posts_{last_post_time[subreddit]}.csv")
        df_comments.to_csv(f"{output_dir}{subreddit}_comments_{last_post_time[subreddit]}.csv")
    iterations+=1