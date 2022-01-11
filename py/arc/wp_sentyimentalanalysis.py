import yahoo_fin.news as news

test=news.get_yf_rss('NXST')

for  i,t  in enumerate(test):
    print(str(i+1) +  '.- ' +t['summary'])