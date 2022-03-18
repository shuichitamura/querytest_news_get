import boto3
from boto3.dynamodb.conditions import Key,Attr
import time
import json

get_news_num = 12

def lambda_handler(event, context):
  print(event)
  try:
    dynamoDB = boto3.resource("dynamodb")
    table = dynamoDB.Table("dx_terminal_news4") # DynamoDBのテーブル名
    # print(event)
    #ソートキーの取得
    try:
        bunki = str(event['params']['querystring']['requestKey'])
        if bunki == '':
            bunki = 'T'
    except:
        bunki = 'T'
    
    # ニュース検索要キーワード抽出
    try:
        word = str(event['params']['querystring']['searchKeyword'])
        if word:
          wordumu = 1
        else:
          wordumu = 0
    except:
        wordumu = 0
    
    
    # lastkeyの取得
    try:
      Last_key_item_name = event['params']['querystring']['item_name']
      Last_key_time_stamp = event['params']['querystring']['timestamp']
      Last_key = {'item_name':Last_key_item_name,'timestamp':Last_key_time_stamp}
      
      # コメントカウントがある場合
      try:
        Last_key_comment_count = int(event['params']['querystring']['comment_count'])
        Last_key = {'item_name':Last_key_item_name,'timestamp':Last_key_time_stamp,'comment_count':Last_key_comment_count}
      except:
        pass
      # like数がある場合
      try:
        Last_key_like_count = int(event['params']['querystring']['like_count'])
        Last_key = {'item_name':Last_key_item_name,'timestamp':Last_key_time_stamp,'like_count':Last_key_like_count}
      except:
        pass
      # 閲覧数がある場合
      try:
        Last_key_view_count = int(event['params']['querystring']['view_count'])
        Last_key = {'item_name':Last_key_item_name,'timestamp':Last_key_time_stamp,'view_count':Last_key_view_count}
      except:
        pass
      
      print(Last_key)
      
      # 検索ワードなしの場合
      if wordumu == 0:
    
      # タイムスタンプによるソート
        if bunki == 'T':
          queryData = table.query(
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            ExclusiveStartKey = Last_key,
            Limit = get_news_num
          )
          
        # コメント数によるソート
        if bunki == 'C':
          queryData = table.query(
            IndexName='comment_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            ExclusiveStartKey = Last_key,
            Limit = get_news_num
          )
        
        # like数によるソート
        if bunki == 'L':
          queryData = table.query(
            IndexName='like_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            ExclusiveStartKey = Last_key,
            Limit = get_news_num
          )
          
        # 閲覧数によるソート
        if bunki == 'V':
          queryData = table.query(
            IndexName='view_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            ExclusiveStartKey = Last_key,
            Limit = get_news_num
          )
      
      # 検索ワードありの場合
      else:
        # タイムスタンプによるソートとキーワードによる抽出
        if bunki == 'T':
          # print(word)
          # print(get_news_num)
          queryData = table.query(
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            FilterExpression = Attr('text').contains(word),
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            ExclusiveStartKey = Last_key,
            Limit = get_news_num
          )
          
        # コメント数によるソートとキーワードによる抽出
        if bunki == 'C':
          # print(word)
          queryData = table.query(
            IndexName='comment_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            FilterExpression = Attr('text').contains(word),
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            ExclusiveStartKey = Last_key,
            Limit = get_news_num
          )
          
        # like数によるソートとキーワードによる抽出
        if bunki == 'L':
          # print(word)
          queryData = table.query(
            IndexName='like_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            FilterExpression = Attr('text').contains(word),
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            ExclusiveStartKey = Last_key,
            Limit = get_news_num
          )
          
          # 閲覧数によるソートとキーワードによる抽出
        if bunki == 'V':
          # print(word)
          queryData = table.query(
            IndexName='view_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            FilterExpression = Attr('text').contains(word),
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            ExclusiveStartKey = Last_key,
            Limit = get_news_num
          )
    
    except:
      # 検索ワードなしの場合
      print(bunki,'z')
      print(wordumu)
      if wordumu == 0:
    
      # タイムスタンプによるソート
        if bunki == 'T':
          queryData = table.query(
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            Limit = get_news_num
          )
          
        # コメント数によるソート
        if bunki == 'C':
          queryData = table.query(
            IndexName='comment_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            Limit = get_news_num
          )
        
        # like数によるソート
        if bunki == 'L':
          queryData = table.query(
            IndexName='like_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            Limit = get_news_num
          )
          
        # 閲覧数によるソート
        if bunki == 'V':
          queryData = table.query(
            IndexName='view_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            Limit = get_news_num
          )
      
      # 検索ワードありの場合
      else:
        # タイムスタンプによるソートとキーワードによる抽出
        if bunki == 'T':
          # print(word)
          # print(get_news_num)
          queryData = table.query(
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            FilterExpression = Attr('text').contains(word),
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            Limit = get_news_num
          )
          
        # コメント数によるソートとキーワードによる抽出
        if bunki == 'C':
          # print(word)
          queryData = table.query(
            IndexName='comment_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            FilterExpression = Attr('text').contains(word),
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            Limit = get_news_num
          )
          
        # like数によるソートとキーワードによる抽出
        if bunki == 'L':
          # print(word)
          queryData = table.query(
            IndexName='like_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            FilterExpression = Attr('text').contains(word),
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            Limit = get_news_num
          )
          
          # 閲覧数によるソートとキーワードによる抽出
        if bunki == 'V':
          # print(word)
          queryData = table.query(
            IndexName='view_count-index',
            KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
            FilterExpression = Attr('text').contains(word),
            ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
            Limit = get_news_num
          )
    
    for w in range(int(queryData["Count"])):
       n1 = str(queryData['Items'][w]["timestamp"])
       response1 = str(n1[0:4]+"/"+n1[4:6]+"/"+n1[6:8])
      # response1 = str(n1[0:4]+"年"+n1[4:6]+"月"+n1[6:8]+"日")
       queryData["Items"][w]["timestamps"] = response1
    
    return queryData
    
  except Exception as e:
        print (e)
