########
#Ultility functions
########
def invert_dict(d):
    e = {}
    for x, y in d.items():
        e.setdefault(y, []).append(x)

    return e
def add_dict(x,y):
    new={ key:x.get(key,[])+y.get(key,[]) for key in set(list(x.keys())+list(y.keys())) }
    for k,v in new.items():
        new[k]= list(set(v))
    return new
