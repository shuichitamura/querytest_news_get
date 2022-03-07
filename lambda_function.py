import boto3
from boto3.dynamodb.conditions import Key,Attr
import time
import json

def lambda_handler(event, context):
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
        wordumu = 1
    except:
        wordumu = 0
    
    # lastkeyの取得
    try:
      Last_key = event['params']['querystring']['lastKey']
      
    except:
      Last_key = []
      # print('e')
    
    Last_key = json.loads(Last_key)
    print(Last_key)
    # もっと見るボタンが押された回数
    try:
      click = int(event['params']['querystring']['clicknum'])
    except:
      click = 0
    
    get_news_num = 12 * (click+1)
    
    # 検索ワードなしの場合
    if wordumu == 0:
  
    # タイムスタンプによるソート
      if bunki == 'T':
        queryData = table.query(
          KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
          ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
          # ExclusiveStartKey = Last_key,
          Limit = get_news_num
        )
        
      # コメント数によるソート
      if bunki == 'C':
        queryData = table.query(
          IndexName='comment_count-index',
          KeyConditionExpression = Key("item_name").eq("news"), # 取得するKey情報
          ScanIndexForward = False, # 昇順か降順か(デフォルトはTrue=昇順)
          # ExclusiveStartKey = Last_key,
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
    
    s = len(queryData["Items"])
    # print(s)
    if click == 0:
      lastkey = queryData["LastEvaluatedKey"]
      queryData = queryData["Items"][0:s]
    else:
      click_s = 12 * click
      click_e = click_s + (s -12*click)
      print(click_s, click_e)
      if click_s == click_e:
        queryData = queryData["Items"][click_s]
      else:
        queryData = queryData["Items"][click_s:click_e]
    return queryData, lastkey
    
  except Exception as e:
        print (e)
