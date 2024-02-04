def writeDf(df,path):
    df.write.format('csv').\
    mode('overwrite').\
    option('path',f'{path}').\
    save()
    