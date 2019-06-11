# -*- coding: utf-8 -*-
import datetime
import time
import sys
import MeCab
import operator
from pymongo import MongoClient
from bson import ObjectId
from itertools import combinations

stop_word={}
DBname="db20141561"
conn=MongoClient("dbpurple.sogang.ac.kr")
db=conn[DBname]
db.authenticate(DBname,DBname)


def make_stop_word():
    f=open("wordList.txt","r")
    while True:
        line=f.readline()
        if not line:
            break
        stop_word[line.strip('\n')]=line.strip('\n')
    f.close()

def morphing(content):
    t=MeCab.Tagger('-d/usr/local/lib/mecab/dic/mecab-ko-dic')
    nodes=t.parseToNode(content.encode('utf-8'))
    MorpList=[]
    while nodes:
        if nodes.feature[0]=='N' and nodes.feature[1]=='N':
            w=nodes.surface
            if not w in stop_word:
                try:
                    w=w.encode('utf-8')
                    MorpList.append(w)
                except:
                    pass
        nodes=nodes.next
    return MorpList

def printMenu():
    print("0. CopyData")
    print("1. Morph")
    print("2. print morphs")
    print("3. print wordset")
    print("4. frequent item set")
    print("5. association rule")

def p0():
    """TODO:
    CopyData news to news_freq
    """

    col1=db['news']
    col2=db['news_freq']

    col2.drop()

    for doc in col1.find():
        contentDic={}
        for key in doc.keys():
            if key!= "_id":
                contentDic[key]=doc[key]
        col2.insert(contentDic)

def p1():
    """TODO:
    Morph news and update news db
    """
    for doc in db['news_freq'].find():
        doc['morph']=morphing(doc['content'])
        db['news_freq'].update({"_id":doc['_id']},doc)

def p2(url):
    """
    TODO:
    input : news url
    output : news morphs
    """    
    for doc in db['news_freq'].find():
        if doc['url']==url:
            for word in doc['morph']:
                print(word)


def p3():
    """
    TODO:
    copy news morph to new db named news_wordset
    """
    col1=db['news_freq']
    col2=db['news_wordset']
    col2.drop()
    for doc in col1.find():
        new_doc={}
        new_set=set()
        for w in doc['morph']:
            new_set.add(w.encode('utf-8'))
        new_doc['word_set']=list(new_set)
        new_doc['url']=doc['url']
        col2.insert(new_doc)

def p4(url):
    """ 
    TODO:
    input : news url
    output : news wordset
    """
    for doc in db['news_wordset'].find():
        if doc['url']==url:
            for word in doc['word_set']:
                print(word)

def p5(length):
    """
    TODO:
    make frequent item_set
    and insert new dbs (dbname=candidate_L+"length")
    ex) 1-th frequent item set dbname = candidate_L1
    """
    db_name="candidate_L"+str(length) 
    candidate=db[db_name]
    candidate.drop()
    
    itemset={}
   
    if length==1:
        for doc in db['news_wordset'].find():
            for word in doc['word_set']:
                if word not in itemset:
                    itemset[word]=1
                else:
                    itemset[word]+=1
        for item in itemset:
            if itemset[item]>=10:
                new_itemset={}
                new_itemset['item_set']=item
                new_itemset['support']=itemset[item]
                candidate.insert(new_itemset)

    elif length==2:
        candidate_one=db['candidate_L1']
        for_candidate_two=[]
        candidate_one_list=[]
        fc=candidate_one.find()
        for i in range(fc.count()-1):
            for j in range(i+1,fc.count()):
                new_list=[]
                doc=fc[i]['item_set']
                doc2=fc[j]['item_set']
                new_list.append(doc)
                new_list.append(doc2)
                for_candidate_two.append(new_list)
        for doc in db['news_wordset'].find():
            for i in range(len(for_candidate_two)):
                if for_candidate_two[i][0] in doc['word_set'] and for_candidate_two[i][1] in doc['word_set']:
                    if frozenset(for_candidate_two[i]) not in itemset:
                        itemset[frozenset(for_candidate_two[i])]=1
                    else:
                        itemset[frozenset(for_candidate_two[i])]+=1
        for item in itemset:
            if itemset[item]>=10:
                new_itemset={}
                new_itemset['item_set']=list(item)
                new_itemset['support']=itemset[item]
                candidate.insert(new_itemset)
    
    elif length==3:
        candidate_two=db['candidate_L2']
        for_candidate_three=[]
        candidate_two_list=[]
        fc=candidate_two.find()
        for i in range(fc.count()-1):
            for j in range(i+1,fc.count()):
                new_list=[]
                doc=""
                doc2=""
                doc3=""
                if fc[i]['item_set'][0]==fc[j]['item_set'][0]:
                    if fc[i]['item_set'][1]==fc[j]['item_set'][1]:
                        continue
                    doc=fc[i]['item_set'][0]
                    doc2=fc[i]['item_set'][1]
                    doc3=fc[j]['item_set'][1]
                elif fc[i]['item_set'][0]==fc[j]['item_set'][1]:
                    if fc[i]['item_set'][1]==fc[j]['item_set'][0]:
                        continue
                    doc=fc[i]['item_set'][0]
                    doc2=fc[i]['item_set'][1]
                    doc3=fc[j]['item_set'][0]
                elif fc[i]['item_set'][1]==fc[j]['item_set'][0]:
                    if fc[i]['item_set'][0]==fc[j]['item_set'][1]:
                        continue
                    doc=fc[i]['item_set'][0]
                    doc2=fc[i]['item_set'][1]
                    doc3=fc[j]['item_set'][1]
                elif fc[i]['item_set'][1]==fc[j]['item_set'][1]:
                    if fc[i]['item_set'][0]==fc[j]['item_set'][0]:
                        continue
                    doc=fc[i]['item_set'][0]
                    doc2=fc[i]['item_set'][1]
                    doc3=fc[j]['item_set'][0]
                if doc=="" or doc2=="" or doc3=="":
                    continue
                new_list.append(doc)
                new_list.append(doc2)
                new_list.append(doc3)
                new_list.sort()
                if new_list in for_candidate_three:
                    continue
                for_candidate_three.append(new_list)
        for doc in db['news_wordset'].find():
            for i in range(len(for_candidate_three)):
                if for_candidate_three[i][0] in doc['word_set'] and for_candidate_three[i][1] in doc['word_set'] and for_candidate_three[i][2] in doc['word_set']:
                    if frozenset(for_candidate_three[i]) not in itemset:
                        itemset[frozenset(for_candidate_three[i])]=1
                    else:
                        itemset[frozenset(for_candidate_three[i])]+=1
        for item in itemset:
            if itemset[item]>=10:
                    new_itemset={}
                    new_itemset['item_set']=list(item)
                    new_itemset['support']=itemset[item]
                    candidate.insert(new_itemset)
                        

def p6(length):
    """
    TODO:
    make strong association rule
    and print all of strong rules
    by length-th frequent itme set
    """
    db_name="candidate_L"+str(length) 
    candidate=db[db_name]

    if length==2:
        for doc in candidate.find():
            total=doc['support']
            first=doc['item_set'][0] 
            second=doc['item_set'][1]
            count1=0
            count2=0
            for n_doc in db['news_wordset'].find():
                if first in n_doc['word_set']:
                    count1+=1
                if second in n_doc['word_set']:
                    count2+=1
            if count1==0:
                continue
            rate=float(float(total)/count1)
            #print(first)
            if rate>=0.5:
                print("%s => %s\t%f"%(first,second,rate))
            if count2==0:
                continue
            rate=float(float(total)/count2)
            if rate>=0.5:
                print("%s => %s\t%f"%(second,first,rate))
    elif length==3: 
        for doc in candidate.find():
            total=doc['support']
            new_list=list(doc['item_set'])
            final_list=[]
            for i in range(len(new_list)-1):
                 for j in range(i+1,len(new_list)):
                    two_list=[]
                    two_list.append(new_list[i])
                    two_list.append(new_list[j])
                    final_list.append(two_list)
            for two in final_list:
                count=0
                for n_doc in db['news_wordset'].find():
                    if two[0] in n_doc['word_set'] and two[1] in n_doc['word_set']:
                        count+=1
                if count==0:
                    continue
                rest_list=list(set(new_list).difference(set(two)))
                rate=float(float(total)/count)
                if rate>=0.5:
                    print("%s , %s => %s\t%f"%(two[0],two[1],rest_list[0],rate))
            
            final_list2=[]
            for i in range(len(new_list)):
                final_list2.append(new_list[i])
            for one in final_list2:
                count=0
                for n_doc in db['news_wordset'].find():
                    if one in n_doc['word_set']:
                        count+=1
                if count==0:
                    continue
                rest_list=list(set(new_list).difference(set(one)))
                rate=float(float(total)/count)
                if rate>=0.5:
                    print("%s => %s , %s\t%f"%(one,rest_list[0],rest_list[1],rate))


if __name__=="__main__":
    make_stop_word()
    printMenu()
    selector=input()
    if selector==0:
        p0()
    elif selector==1:
        p1()
        p3()
    elif selector==2:
        url=str(raw_input("input news url:"))
        p2(url)
    elif selector==3:
        url=str(raw_input("input news url:"))
        p4(url)
    elif selector==4:
        length=int(raw_input("input length of the frequent item:"))
        p5(length)
    elif selector==5:
        length=int(raw_input("input length of the frequent item:"))
        p6(length)

