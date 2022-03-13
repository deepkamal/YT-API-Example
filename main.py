from typing import Union

from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime

API_KEY = 'AIzaSyBz7mtQkhtm5tfAuyzKS5DXppXgXLmPx8c'
youtube = build('youtube', 'v3', developerKey=API_KEY)


def fetch_comments_on_video(video_id):
    comments_df = pd.DataFrame(columns=['video_id', 'author_id', 'author_display_name', 'comment_text',
                                        'post_time', 'upd_time', 'likes', 'replies', 'commentId', 'parentId'])
    comments = []
    # retrieve youtube video results
    video_response = youtube.commentThreads().list(
        part='snippet,replies',
        videoId=video_id
    ).execute()

    fetch_count = 0
    while video_response:
        for item in video_response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            this_comment = {'video_id': video_id,
                            'author_id': comment['authorChannelId']['value'] if 'authorChannelId' in comment else "",
                            'author_display_name': comment['authorDisplayName'],
                            'comment_text': comment['textOriginal'],
                            'post_time': comment['publishedAt'],
                            'upd_time': comment['updatedAt'],
                            'likes': comment['likeCount'],
                            'replies': item['snippet']['totalReplyCount'],
                            'commentId': item['id'],
                            'parentId': None}
            comments.append(this_comment)
            # comments_df = comments_df.append(this_comment, ignore_index=True)
            # counting number of reply of comment
            replies = item['snippet']['totalReplyCount']
            if (replies > 0) and ('replies' in item):
                for reply in item['replies']['comments']:
                    reply_comment = {'video_id': reply['snippet']['videoId'],
                                     'author_id': reply['snippet']['authorChannelId']['value']
                                     if 'authorChannelId' in reply['snippet'] else "",
                                     'author_display_name': reply['snippet']['authorDisplayName'],
                                     'comment_text': reply['snippet']['textOriginal'],
                                     'post_time': reply['snippet']['publishedAt'],
                                     'upd_time': reply['snippet']['updatedAt'],
                                     'likes': reply['snippet']['likeCount'],
                                     'replies': 0,
                                     'commentId': reply['id'],
                                     'parentId': reply['snippet']['parentId']}
                    comments.append(reply_comment)
        if ('nextPageToken' in video_response) and fetch_count < 100:
            fetch_count += 1
            video_response = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id,
                pageToken=video_response['nextPageToken']
            ).execute()
        else:
            fetch_count = 0
            break
    return pd.DataFrame.from_records(comments)


def list_videos_of_channel(channel_id):
    video_ids = []
    ch_request = youtube.channels().list(
        part='statistics,contentDetails',
        id=channel_id)

    nextPageToken = None

    # Channel Information
    ch_response = ch_request.execute()
    # print(ch_response)
    vid = ch_response['items'][0]['statistics']['videoCount']
    playlistId = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    print("Total Number of Videos in channel :", vid)

    while True:
        # Retrieve youtube video results
        pl_request = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlistId,
            maxResults=50,
            pageToken=nextPageToken
        )
        pl_response = pl_request.execute()

        # Iterate through all response and get video description
        for item in pl_response['items']:
            video_id = item['snippet']['resourceId']['videoId']
            video_ids.append(video_id)

        nextPageToken = pl_response.get('nextPageToken')

        if not nextPageToken:
            break

    return video_ids


done_video_ids = {}

done_videos_df = pd.read_csv('done_vids.csv')

# print(done_videos_df)

print("Already done videos Found:\n", done_videos_df)

time_ts = datetime.now().strftime('%Y%m%d%H%M%S')

if __name__ == "__main__":
    SONY_MUSIC_INDIA_CHANNEL_ID = 'UC56gTxNs4f9xZ7Pa2i5xNzg'
    AKSHAT_ZAYN_CHANNEL_ID = 'UCqW8jxh4tH1Z1sWPbkGWL4g'


    processing_channel = AKSHAT_ZAYN_CHANNEL_ID

    print("Already done videos for this channel :\n", done_videos_df[done_videos_df['channel_id']==processing_channel])

    comment_df = pd.DataFrame(columns=['video_id', 'author_id', 'author_display_name', 'comment_text',
                                       'post_time', 'upd_time', 'likes', 'replies', 'commentId', 'parentId'])
    channel_videos = list_videos_of_channel(processing_channel)
    for aVideoId in channel_videos:
        try:
            if done_videos_df[done_videos_df['videoId'] == aVideoId]['videoId'].count() == 0:
                print('Fetching comments for videoId', aVideoId)
                fetch_comments_on_video(aVideoId).to_csv('comments/' + aVideoId + time_ts + '.csv')
                # comment_df = pd.concat([comment_df, fetch_comments_on_video(aVideoId)], sort=False)
                #
                # comment_df.to_csv('AllComments-' + (datetime.now().strftime('%Y%m%d%H%M%S')) + '.csv')
                done_videos_df = done_videos_df.append({'channelId': processing_channel,
                                                        'videoId': aVideoId},
                                                       ignore_index=True)
                done_videos_df[['channelId', 'videoId']].to_csv('done_vids.csv')
            else:
                print('comments for videoId', aVideoId, 'already loaded,skipping...')
        except Exception as e:
            print('Error occurred for', aVideoId)
            print(e)
