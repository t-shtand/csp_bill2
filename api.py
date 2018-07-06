import flask
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, make_response, Response, redirect, session
import requests, json
from flask_jsonpify import jsonpify
import io
import csv
import asyncio
from aiohttp import ClientSession
from pandas.io.json import json_normalize
import datetime

app = Flask(__name__)
app.secret_key = "jkl"

@app.route('/')
@app.route('/index')
def index():
	my_variab = "system"
	return flask.render_template('index.html', variab = my_variab )

@app.route('/send', methods= ['GET','POST'])
def send():
	if request.method == 'POST':
		'''accountId = request.form['accountId']
		appId = request.form['appId']
		appSecret = request.form['appSecret']'''
		
		session['accountId'] = request.form['accountId']
		session['appId'] = request.form['appId']
		session['appSecret'] = request.form['appSecret']
		session['startDate'] = request.form['startDate']
		session['endDate'] = request.form['endDate']
        
		return flask.render_template('afterRequest.html', PaccountId = session['accountId'] , PappId = session['appId'], PappSecret = session['appSecret']  )

		#return flask.render_template('afterRequest.html', PaccountId = accountId, PappId = appId, PappSecret = appSecret )

def getToken():
	#Token Api
	accountId = session.get('accountId', None)
	appId = session.get('appId', None)
	appSecret = session.get('appSecret', None)
	url_token = "https://login.windows.net/" + accountId + "/oauth2/token"
	data = {
	"grant_type":"client_credentials",
	"client_id":appId,
	"client_secret":appSecret,
	"resource":"https://graph.windows.net"
	}
	headers  = {"Content-Type": "application/x-www-form-urlencoded"}
	response_json = requests.post(url_token, data = data, headers = headers)

	if(response_json.ok):
		jData = json.loads(response_json.text)
		#session['accesToken'] =  jData['access_token']
		return jData['access_token']

	else :
		#return response_json.raise_for_status()
		#form a 404 page and give a link to go back to enter credentials
		return "Please enter valid credentials"

customerDict = {}

def getCustomers():
	#Token Api
	access_token = session.get('accesToken', None)
	url_customers = "https://api.partnercenter.microsoft.com/v1/customers?size=200"
	headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
	response_json = requests.get(url_customers, headers = headers)
	response_json.encoding = 'utf-8-sig'

	if(response_json.ok):
		jData = json.loads(response_json.text)
		#totalCoun = "Count is : " + str(jData['totalCount'])
		itemsArray = jData["items"]
		#allitems = ""
		customerList = []
		
		#i=1
		for item in itemsArray:
			#allitems = allitems  + str(i) + "  " +  item["id"] + " \n "
			customerId = item["id"]
			customerList.append(customerId)
			customerDict[customerId]={}
			customerDict[customerId]["domain"] = item["companyProfile"]["domain"]
			customerDict[customerId]["companyName"] = item["companyProfile"]["companyName"]

			#i = i+1

		#return allitems
		return customerList
		#return jsonpify(jData)

	else :
		#return response_json.raise_for_status()
		#form a 404 page and give a link to go back to enter credentials
		return "Please enter valid credentials"


def getCustomers_multiuser(customerDict_multiuser):
	#Token Api
	access_token = session.get('accesToken', None)
	url_customers = "https://api.partnercenter.microsoft.com/v1/customers?size=200"
	headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
	response_json = requests.get(url_customers, headers = headers)
	response_json.encoding = 'utf-8-sig'

	if(response_json.ok):
		jData = json.loads(response_json.text)
		#totalCoun = "Count is : " + str(jData['totalCount'])
		itemsArray = jData["items"]
		#allitems = ""
		customerList = []
		
		#i=1
		for item in itemsArray:
			#allitems = allitems  + str(i) + "  " +  item["id"] + " \n "
			customerId = item["id"]
			customerList.append(customerId)
			customerDict_multiuser[customerId]={}
			customerDict_multiuser[customerId]["domain"] = item["companyProfile"]["domain"]
			customerDict_multiuser[customerId]["companyName"] = item["companyProfile"]["companyName"]

			#i = i+1

		#return allitems
		return customerList
		#return jsonpify(jData)

	else :
		#return response_json.raise_for_status()
		#form a 404 page and give a link to go back to enter credentials
		return "Please enter valid credentials"



listPriceDict = {}

def getListPrice():
	#Token Api
	access_token = session.get('accesToken', None)
	url_price = "https://api.partnercenter.microsoft.com/v1/ratecards/azure?currency=INR&region=IN"
	headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
	response_json = requests.get(url_price, headers = headers)
	response_json.encoding = 'utf-8-sig'

	if(response_json.ok):
		jData = json.loads(response_json.text)
		itemsArray = jData["meters"]
		
		for item in itemsArray:
			resourceId = item["id"]
			listPriceDict[resourceId] = item["rates"]

	else :
		#return response_json.raise_for_status()
		#form a 404 page and give a link to go back to enter credentials
		return "Please enter valid credentials"

def getListPrice_multiuser(listPriceDict_multiuser):
	#Token Api
	access_token = session.get('accesToken', None)
	url_price = "https://api.partnercenter.microsoft.com/v1/ratecards/azure?currency=INR&region=IN"
	headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
	response_json = requests.get(url_price, headers = headers)
	response_json.encoding = 'utf-8-sig'

	if(response_json.ok):
		jData = json.loads(response_json.text)
		itemsArray = jData["meters"]
		
		for item in itemsArray:
			resourceId = item["id"]
			listPriceDict[resourceId] = item["rates"]

	else :
		#return response_json.raise_for_status()
		#form a 404 page and give a link to go back to enter credentials
		return "Please enter valid credentials"


'''def getSubscriptions(customerList):
	#Token Api
	access_token = session.get('accesToken', None)
	#url_subscriptions = "https://api.partnercenter.microsoft.com/v1/customers?size=200"
	headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}

	i = 0
	partnerDict = {}

	for customerId in customerList:
		i = i+1
		if(i >=5):
			break

		url_subscriptions = "https://api.partnercenter.microsoft.com/v1/customers/" + customerId + "/subscriptions"
		response_json = requests.get(url_subscriptions, headers = headers)
		response_json.encoding = 'utf-8-sig'
		subscriptionsList = []

		if(response_json.ok):
			jData = json.loads(response_json.text)
			itemsArray  = jData["items"]
			for item in itemsArray:
				subscriptionsList.append(item["id"])

		else :
			return "Please enter valid credentials"

		partnerDict[customerId] = subscriptionsList

	return partnerDict
	#return customerList'''

partnerDict = {}

def getPartnerDict(customerList):
    """Fetch list of web pages asynchronously."""
    #start_time = default_timer()
    #loop = asyncio.get_event_loop() # event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(fetch_all(customerList)) # tasks to do
    loop.run_until_complete(future) # loop until done
    #tot_elapsed = default_timer() - start_time
    #print(' WITH ASYNCIO: '.rjust(30, '-') + '{0:5.2f} {1}'. \
        #format(tot_elapsed, asterisks(tot_elapsed)))


async def fetch_all(customerList):

    """Launch requests for all web pages."""
    tasks = []
    access_token = session.get('accesToken', None)
    headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
    #fetch.start_time = dict() # dictionary of start times for each url
    async with ClientSession(headers = headers) as cSession:

        for customerId in customerList:
            task = asyncio.ensure_future(fetch(customerId, cSession))
            tasks.append(task) # create list of tasks

        responses = await asyncio.gather(*tasks) # gather task responses


async def fetch(customerId, cSession):

    """Fetch a url, using specified ClientSession."""
    #fetch.start_time[url] = default_timer()
    url = "https://api.partnercenter.microsoft.com/v1/customers/" + customerId + "/subscriptions"
    async with cSession.get(url) as response:
        resp = await response.text(encoding = 'utf-8-sig')
        subscriptionsList = []
        jData = json.loads(resp)
        try:
	        itemsArray  = jData["items"]
	        for item in itemsArray:
	        	subscriptionId = item["id"]
	        	subscriptionsList.append(item["id"])
	        	customerDict[customerId][subscriptionId] = {}
	        	#customerDict[customerId][subscriptionId]["id"] = subscriptionId
	        	customerDict[customerId][subscriptionId]["offerName"] = item["offerName"]
	        	customerDict[customerId][subscriptionId]["billingType"] = item["billingType"]
	        	customerDict[customerId][subscriptionId]["billingCycle"] = item["billingCycle"]



	        partnerDict[customerId] = subscriptionsList
	        #print(resp)
	        return resp
        except:
        	return resp
    	#elapsed = default_timer() - fetch.start_time[url]
    	#print('{0:30}{1:5.2f} {2}'.format(url, elapsed, asterisks(elapsed)))

def getPartnerDict_multiuser(customerList,customerDict_multiuser, partnerDict_multiuser):
    """Fetch list of web pages asynchronously."""
    #start_time = default_timer()
    #loop = asyncio.get_event_loop() # event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(fetch_all_multiuser(customerList,customerDict_multiuser, partnerDict_multiuser)) # tasks to do
    loop.run_until_complete(future) # loop until done
    #tot_elapsed = default_timer() - start_time
    #print(' WITH ASYNCIO: '.rjust(30, '-') + '{0:5.2f} {1}'. \
        #format(tot_elapsed, asterisks(tot_elapsed)))


async def fetch_all_multiuser(customerList,customerDict_multiuser, partnerDict_multiuser):

    """Launch requests for all web pages."""
    tasks = []
    access_token = session.get('accesToken', None)
    headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
    #fetch.start_time = dict() # dictionary of start times for each url
    async with ClientSession(headers = headers) as cSession:

        for customerId in customerList:
            task = asyncio.ensure_future(fetch_multiuser(customerId, cSession,customerDict_multiuser, partnerDict_multiuser))
            tasks.append(task) # create list of tasks

        responses = await asyncio.gather(*tasks) # gather task responses


async def fetch_multiuser(customerId, cSession, customerDict_multiuser, partnerDict_multiuser):

    """Fetch a url, using specified ClientSession."""
    #fetch.start_time[url] = default_timer()
    url = "https://api.partnercenter.microsoft.com/v1/customers/" + customerId + "/subscriptions"
    async with cSession.get(url) as response:
        resp = await response.text(encoding = 'utf-8-sig')
        subscriptionsList = []
        jData = json.loads(resp)
        try:
	        itemsArray  = jData["items"]
	        for item in itemsArray:
	        	subscriptionId = item["id"]
	        	subscriptionsList.append(item["id"])
	        	customerDict_multiuser[customerId][subscriptionId] = {}
	        	#customerDict[customerId][subscriptionId]["id"] = subscriptionId
	        	customerDict_multiuser[customerId][subscriptionId]["offerName"] = item["offerName"]
	        	customerDict_multiuser[customerId][subscriptionId]["billingType"] = item["billingType"]
	        	customerDict_multiuser[customerId][subscriptionId]["billingCycle"] = item["billingCycle"]



	        partnerDict_multiuser[customerId] = subscriptionsList
	        #print(resp)
	        return resp
        except:
        	return resp
    	#elapsed = default_timer() - fetch.start_time[url]
    	#print('{0:30}{1:5.2f} {2}'.format(url, elapsed, asterisks(elapsed)))

#allrecords = ""
allrecords = []
resultDf = pd.DataFrame()

def getRecords(utilizationRecordUrls):
    """Fetch list of web pages asynchronously."""
    
    #loop = asyncio.new_event_loop()
    #asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    #future = asyncio.ensure_future(fetch_all2(utilizationRecordUrls)) # tasks to do
    future = asyncio.ensure_future(fetch_all3(utilizationRecordUrls)) # ta
    loop.run_until_complete(future) # loop until done
    loop.close()

def getRecords_multiuser(dateList,resultDf_multiuser, customerDict_multiuser, partnerDict_multiuser):
    """Fetch list of web pages asynchronously."""
    
    #loop = asyncio.new_event_loop()
    #asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    #future = asyncio.ensure_future(fetch_all2(utilizationRecordUrls)) # tasks to do
    future = asyncio.ensure_future(fetch_all3_multiuser(dateList,resultDf_multiuser, customerDict_multiuser, partnerDict_multiuser)) # ta
    loop.run_until_complete(future) # loop until done
    loop.close()

def getRecordsTemp(utilizationRecordUrls, dateList):
    """Fetch list of web pages asynchronously."""
    
    #loop = asyncio.new_event_loop()
    #asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    #future = asyncio.ensure_future(fetch_all2(utilizationRecordUrls)) # tasks to do
    future = asyncio.ensure_future(fetch_all3Temp(utilizationRecordUrls, dateList)) # ta
    loop.run_until_complete(future) # loop until done
    loop.stop()
    loop.close()

'''async def fetch_all2(utilizationRecordUrls):

    """Launch requests for all web pages."""
    tasks = []
    access_token = session.get('accesToken', None)
    headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
    #fetch.start_time = dict() # dictionary of start times for each url
    async with ClientSession(headers = headers) as cSession:

        for url in utilizationRecordUrls:
        	task = asyncio.ensure_future(fetch2(url, cSession))
        	tasks.append(task) # create list of tasks

    	#global allrecords 
        allrecords1 = await asyncio.gather(*tasks) # gather task responses
'''
async def fetch_all3(utilizationRecordUrls):

    """Launch requests for all web pages."""
    tasks = []
    sem = asyncio.Semaphore(1000)
    access_token = session.get('accesToken', None)
    

    headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
    #fetch.start_time = dict() # dictionary of start times for each url
    async with ClientSession(headers = headers) as cSession:

        for customerId, subsArray in partnerDict.items():
        	for subscriptionId in subsArray:
        		task = asyncio.ensure_future(fetch3_sem(sem,cSession,customerId,subscriptionId))
        		tasks.append(task)

    	#global allrecords 
        allrecords1 = await asyncio.gather(*tasks) # gather task responses

async def fetch_all3_multiuser(dateList,resultDf_multiuser, customerDict_multiuser, partnerDict_multiuser):

    """Launch requests for all web pages."""
    tasks = []
    sem = asyncio.Semaphore(1000)
    access_token = session.get('accesToken', None)
    

    headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
    #fetch.start_time = dict() # dictionary of start times for each url    
    async with ClientSession(headers = headers) as cSession:
    	for eachDate in dateList:
	        for customerId, subsArray in partnerDict_multiuser.items():
	        	for subscriptionId in subsArray:
	        		task = asyncio.ensure_future(fetch3_sem_multiuser(sem,cSession,customerId,subscriptionId,eachDate,resultDf_multiuser,customerDict_multiuser))
	        		tasks.append(task)

    	#global allrecords
    	allrecords1 = await asyncio.gather(*tasks) # gather task responses


async def fetch_all3Temp(utilizationRecordUrls, dateList):

    """Launch requests for all web pages."""
    tasks = []
    sem = asyncio.Semaphore(1000)
    access_token = session.get('accesToken', None)
    

    headers  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
    #fetch.start_time = dict() # dictionary of start times for each url
    async with ClientSession(headers = headers) as cSession:
    	for eachDate in dateList:
	        for customerId, subsArray in partnerDict.items():
	        	for subscriptionId in subsArray:
	        		task = asyncio.ensure_future(fetch3Temp1(sem,cSession,customerId,subscriptionId,eachDate))
	        		tasks.append(task)

    	#global allrecords
    	allrecords1 = await asyncio.gather(*tasks) # gather task responses

async def fetch3_sem(sem, cSession,customerId,subscriptionId):
	async with sem:
		await fetch3(cSession,customerId,subscriptionId)
  
async def fetch3_sem_multiuser(sem, cSession,customerId,subscriptionId,eachDate, resultDf_multiuser, customerDict_multiuser):
	async with sem:
		await fetch3_multiuser(cSession,customerId,subscriptionId,eachDate,resultDf_multiuser, customerDict_multiuser)


async def fetch3Temp1(sem, cSession,customerId,subscriptionId,eachDate):
	async with sem:
		await fetch3Temp(cSession,customerId,subscriptionId,eachDate)
  

async def fetch3(cSession,customerId,subscriptionId):

	#startTime = "2018-05-20"
	#endTime = "2018-06-19"
	startTime = session.get('startDate',None)
	endTime = session.get('endDate',None)
	url_ = "https://api.partnercenter.microsoft.com/v1/customers/" + customerId + "/subscriptions/" + subscriptionId +"/utilizations/azure?start_time=" + startTime + "&end_time=" + endTime

	"""Fetch a url, using specified ClientSession."""
	async with cSession.get(url_) as response:
		resp = await response.text(encoding = 'utf-8-sig')
		jData = json.loads(resp)

		#for item in jData["items"]:
		try:
			if(jData["totalCount"] > 0):
			#if(len(jData["items"]) > 0):
				allRowsDf = json_normalize(jData["items"])
				allRowsDf["customerId"] = customerId
				allRowsDf["customerCompanyName"] = customerDict[customerId]["companyName"]
				allRowsDf["domain"] = customerDict[customerId]["domain"]
				allRowsDf["billingType"] = customerDict[customerId][subscriptionId]["billingType"]
				allRowsDf["billingCycle"] = customerDict[customerId][subscriptionId]["billingCycle"]
				allRowsDf["offerName"] = customerDict[customerId][subscriptionId]["offerName"]
				allRowsDf["subscriptionId"] = subscriptionId
				#normalisedJson = json.loads(allRowsDf.to_json())
				#allrecords.append(normalisedJson)
				global resultDf
				resultDf = resultDf.append(allRowsDf,sort = True)

				try:
					while "next" in jData["links"]:
						access_token = session.get('accesToken', None)
						headersNext  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
						print(jData["links"]["next"]["uri"])
						#print(jData["links"]["next"]["headers"])
						next_url = "https://api.partnercenter.microsoft.com/v1/" + jData["links"]["next"]["uri"]
						headersArr= jData["links"]["next"]["headers"]
						for item1 in headersArr:
							print(item1["value"])
							headersNext[item1["key"]] = item1["value"]

						response_json = requests.get(next_url, headers = headersNext)
						response_json.encoding = 'utf-8-sig'
						jData = json.loads(response_json.text)
						print(jData["totalCount"])
						allRowsDf = json_normalize(jData["items"])
						allRowsDf["customerId"] = customerId
						allRowsDf["customerCompanyName"] = customerDict[customerId]["companyName"]
						allRowsDf["domain"] = customerDict[customerId]["domain"]
						allRowsDf["billingType"] = customerDict[customerId][subscriptionId]["billingType"]
						allRowsDf["billingCycle"] = customerDict[customerId][subscriptionId]["billingCycle"]
						allRowsDf["offerName"] = customerDict[customerId][subscriptionId]["offerName"]
						allRowsDf["subscriptionId"] = subscriptionId
						#normalisedJson = json.loads(allRowsDf.to_json())
						#allrecords.append(normalisedJson)
						#global resultDf
						resultDf = resultDf.append(allRowsDf,sort = True)

						

				except:
					pass

			#allrecords.append(jData)
			return resp
		except:
			return resp

async def fetch3_multiuser(cSession,customerId,subscriptionId,eachDate,resultDf_multiuser, customerDict_multiuser):

	#startTime = "2018-05-20"
	#endTime = "2018-06-19"
	startTime = eachDate
	accountId = session.get('accountId', None)
	#number_pkl_file = session.get('number_pkl_file',None)
	#string_pickle_filename = accountId +  "_"+ str(number_pkl_file) +  ".pkl"
	#endTime = "2018-06-19"
	string_pickle_filename  = session.get('string_pickle_filename',None)
	tempDate  = datetime.datetime.strptime(startTime,"%Y-%m-%d")
	tempFinalDate = tempDate + datetime.timedelta(days=3)
	endTime = str(tempFinalDate.strftime('%Y-%m-%d'))
	url_ = "https://api.partnercenter.microsoft.com/v1/customers/" + customerId + "/subscriptions/" + subscriptionId +"/utilizations/azure?start_time=" + startTime + "&end_time=" + endTime

	"""Fetch a url, using specified ClientSession."""
	async with cSession.get(url_) as response:
		resp = await response.text(encoding = 'utf-8-sig')
		jData = json.loads(resp)

		#for item in jData["items"]:
		try:
			if(jData["totalCount"] > 0):
			#if(len(jData["items"]) > 0):
				allRowsDf = json_normalize(jData["items"])
				allRowsDf["customerId"] = customerId
				allRowsDf["customerCompanyName"] = customerDict_multiuser[customerId]["companyName"]
				allRowsDf["domain"] = customerDict_multiuser[customerId]["domain"]
				allRowsDf["billingType"] = customerDict_multiuser[customerId][subscriptionId]["billingType"]
				allRowsDf["billingCycle"] = customerDict_multiuser[customerId][subscriptionId]["billingCycle"]
				allRowsDf["offerName"] = customerDict_multiuser[customerId][subscriptionId]["offerName"]
				allRowsDf["subscriptionId"] = subscriptionId
				#normalisedJson = json.loads(allRowsDf.to_json())
				#allrecords.append(normalisedJson)
				#global resultDf
				#temp_dict_multiuser = allRowsDf.to_dict()
				#resultDf_multiuser.update(temp_dict_multiuser)
				df_pickle_multi = pd.read_pickle(string_pickle_filename)
				df_pickle_multi = df_pickle_multi.append(allRowsDf,sort = True)
				df_pickle_multi.to_pickle(string_pickle_filename)
				#resultDf_multiuser = resultDf_multiuser.append(allRowsDf,sort = True)

				try:
					while "next" in jData["links"]:
						access_token = session.get('accesToken', None)
						headersNext  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
						print(jData["links"]["next"]["uri"])
						#print(jData["links"]["next"]["headers"])
						next_url = "https://api.partnercenter.microsoft.com/v1/" + jData["links"]["next"]["uri"]
						headersArr= jData["links"]["next"]["headers"]
						for item1 in headersArr:
							print(item1["value"])
							headersNext[item1["key"]] = item1["value"]

						response_json = requests.get(next_url, headers = headersNext)
						response_json.encoding = 'utf-8-sig'
						jData = json.loads(response_json.text)
						print(jData["totalCount"])
						allRowsDf = json_normalize(jData["items"])
						allRowsDf["customerId"] = customerId
						allRowsDf["customerCompanyName"] = customerDict_multiuser[customerId]["companyName"]
						allRowsDf["domain"] = customerDict_multiuser[customerId]["domain"]
						allRowsDf["billingType"] = customerDict_multiuser[customerId][subscriptionId]["billingType"]
						allRowsDf["billingCycle"] = customerDict_multiuser[customerId][subscriptionId]["billingCycle"]
						allRowsDf["offerName"] = customerDict_multiuser[customerId][subscriptionId]["offerName"]
						allRowsDf["subscriptionId"] = subscriptionId
						#normalisedJson = json.loads(allRowsDf.to_json())
						#allrecords.append(normalisedJson)
						#global resultDf
						#temp_dict_multiuser = allRowsDf.to_dict()
						#resultDf_multiuser.update(temp_dict_multiuser)
						#resultDf_multiuser = resultDf_multiuser.append(allRowsDf,sort = True)
						df_pickle_multi = pd.read_pickle(string_pickle_filename)
						df_pickle_multi = df_pickle_multi.append(allRowsDf,sort = True)
						df_pickle_multi.to_pickle(string_pickle_filename)

						

				except:
					pass

			#allrecords.append(jData)
			return resp
		except:
			return resp


  	
async def fetch3Temp(cSession,customerId,subscriptionId, eachDate):

	#startTime = "2018-05-20"
	startTime = eachDate
	#endTime = "2018-06-19"
	tempDate  = datetime.datetime.strptime(startTime,"%Y-%m-%d")
	tempFinalDate = tempDate + datetime.timedelta(days=3)
	endTime = str(tempFinalDate.strftime('%Y-%m-%d'))
	url_ = "https://api.partnercenter.microsoft.com/v1/customers/" + customerId + "/subscriptions/" + subscriptionId +"/utilizations/azure?start_time=" + startTime + "&end_time=" + endTime

	"""Fetch a url, using specified ClientSession."""
	async with cSession.get(url_) as response:
		resp = await response.text(encoding = 'utf-8-sig')
		jData = json.loads(resp)

		#for item in jData["items"]:
		try:
			if(jData["totalCount"] > 0):
			#if(len(jData["items"]) > 0):
				allRowsDf = json_normalize(jData["items"])
				allRowsDf["customerId"] = customerId
				allRowsDf["customerCompanyName"] = customerDict[customerId]["companyName"]
				allRowsDf["domain"] = customerDict[customerId]["domain"]
				allRowsDf["billingType"] = customerDict[customerId][subscriptionId]["billingType"]
				allRowsDf["billingCycle"] = customerDict[customerId][subscriptionId]["billingCycle"]
				allRowsDf["offerName"] = customerDict[customerId][subscriptionId]["offerName"]
				allRowsDf["subscriptionId"] = subscriptionId
				#normalisedJson = json.loads(allRowsDf.to_json())
				#allrecords.append(normalisedJson)
				global resultDf
				resultDf = resultDf.append(allRowsDf,sort = True)

				try:
					while "next" in jData["links"]:
						access_token = session.get('accesToken', None)
						headersNext  = {"Authorization" : "Bearer " + access_token ,"Accept": "application/json"}
						print(jData["links"]["next"]["uri"])
						#print(jData["links"]["next"]["headers"])
						next_url = "https://api.partnercenter.microsoft.com/v1/" + jData["links"]["next"]["uri"]
						headersArr= jData["links"]["next"]["headers"]
						for item1 in headersArr:
							print(item1["value"])
							headersNext[item1["key"]] = item1["value"]

						response_json = requests.get(next_url, headers = headersNext)
						response_json.encoding = 'utf-8-sig'
						jData = json.loads(response_json.text)
						print(jData["totalCount"])
						allRowsDf = json_normalize(jData["items"])
						allRowsDf["customerId"] = customerId
						allRowsDf["customerCompanyName"] = customerDict[customerId]["companyName"]
						allRowsDf["domain"] = customerDict[customerId]["domain"]
						allRowsDf["billingType"] = customerDict[customerId][subscriptionId]["billingType"]
						allRowsDf["billingCycle"] = customerDict[customerId][subscriptionId]["billingCycle"]
						allRowsDf["offerName"] = customerDict[customerId][subscriptionId]["offerName"]
						allRowsDf["subscriptionId"] = subscriptionId
						#normalisedJson = json.loads(allRowsDf.to_json())
						#allrecords.append(normalisedJson)
						#global resultDf
						resultDf = resultDf.append(allRowsDf,sort = True)

						

				except:
					pass


			#allrecords.append(jData)
			return resp
		except:
			return resp

'''async def fetch2(url, cSession):

    """Fetch a url, using specified ClientSession.""" 
    async with cSession.get(url) as response:
        resp = await response.text(encoding = 'utf-8-sig')
        jData = json.loads(resp)

        #for item in jData["items"]:
        if(jData["totalCount"] > 0):
        	allRowsDf = json_normalize(jData["items"])
        	#normalisedJson = json.loads(allRowsDf.to_json())
        	#allrecords.append(normalisedJson)
        	global resultDf
        	resultDf = resultDf.append(allRowsDf, sort = True)

        #allrecords.append(jData)
        return resp
'''     	

def getRecordsUrls():
	
	#startTime =  session.get('startTime', None)
	#endTime =  session.get('endTime', None)
	startTime = "2018-04-10"
	endTime = "2018-04-12"

	utilizationRecordUrls = []
	for customerId, subsArray in partnerDict.items():
		for subscriptionId in subsArray:
			url = "https://api.partnercenter.microsoft.com/v1/customers/" + customerId + "/subscriptions/" + subscriptionId +"/utilizations/azure?start_time=" + startTime + "&end_time=" + endTime
			utilizationRecordUrls.append(url)

	return utilizationRecordUrls


def extractResGrp(resUri):
	temp = 0
	if(resUri.find("resourceGroups/") != -1):
		temp = resUri.find("resourceGroups/")
	else:
		temp = resUri.find("resourcegroups/")

	startIndex = temp + 15
	endIndex = resUri.find("/providers",startIndex)
	return resUri[startIndex:endIndex].upper()

def extractResType(resUri):
	temp = 0
	startIndex = resUri.rfind("/") +1
	#endIndex = resUri.find("/providers",startIndex)
	return resUri[startIndex:].upper()

def mergePriceList(resourceId, quantity):
	try:
		ratesDict = listPriceDict[resourceId]
		quantList = sorted(list(ratesDict.keys()))
		resultquant = "0"
		size= len(quantList)
		if(size > 1):
			for i in range(size):
				if(i+1 < size and quantity < float(quantList[i+1]) ):
					resultquant = quantList[i]
					break
				elif(i+1 ==size):
					resultquant  =quantList[i]
				i +=1

		else:
			resultquant = quantList[0]

		return ratesDict[resultquant]
	except:
		return "NA"

def extractTags(allTags):
	'''listTags = allTags.split(',')
	strTemp = ''.join(listTags)
	listT = strTemp.split(' ')
	cleanedList = [x for x in listT if (str(x) != 'nan' and str(x) != '[nan' and str(x) != 'nan]')]
	tempstr  = str(' '.join(cleanedList))
	if(tempstr.endswith("]")):
		return tempstr[:-1]
	elif(tempstr.startswith("[")):
		tempstr2  = tempstr[1:]
		if(tempstr2.startswith("[")):
			return tempstr[2:]
		else:
			return tempstr[1:]
	else:
		return tempstr'''
	if(allTags != "nan"):
		return allTags
	else:
		return ""


def extractTags_(myString):

	tempStr = myString.replace("nan","")
	if(len(tempStr) > 0 and tempStr[8] != '-' and tempStr[13] != '-'):
		return tempStr
	else:
		return ""


def getDates():
	startTime = session.get('startDate',None)
	endTime = session.get('endDate',None)
	#noDays = session.get('noDays',None)
	delt = datetime.datetime.strptime(endTime,"%Y-%m-%d") - datetime.datetime.strptime(startTime,"%Y-%m-%d")
	noDays = delt.days + 1
	factor = 3
	size  = noDays/3 - 1
	dateList = []
	size_parts  =9

	tempDate  = datetime.datetime.strptime(startTime,"%Y-%m-%d")
	dateList.append(tempDate.strftime('%Y-%m-%d'))
	for i in range(int(size)):
		tempFinalDate = tempDate + datetime.timedelta(days=factor)
		tempFinalDate1 = tempFinalDate.strftime('%Y-%m-%d')
		dateList.append(tempFinalDate1)
		tempDate = tempFinalDate

	return dateList

number_pkl_file = 0

@app.route('/download', methods= ['GET','POST'])
def download():
	if request.method == 'POST':
	
		session['accesToken'] = getToken()
		customerDictTemp = {}
		accountId = session.get('accountId', None)
		global number_pkl_file
		number_pkl_file +=1
		session['string_pickle_filename'] =accountId + "_"+ str(number_pkl_file) +  ".pkl"

		string_pickle_filename  =session.get('string_pickle_filename',None)
		#string_pickle_filename = accountId + "_"+ str(session['number_pkl_file']) +  ".pkl"



		#session['startDate'] = "2018-05-11"
		#session['endDate'] = "2018-06-10"
		#session['noDays'] = 30
		#return str(session.get('endDate'))
		global resultDf
		tempDf = pd.DataFrame()
		resultDf  = tempDf

		global customerDict
		tempCustDict = {}
		customerDict = tempCustDict

		global listPriceDict
		tempListPriceDict = {}
		listPriceDict = tempListPriceDict

		global partnerDict
		tempPartDict = {}
		partnerDict  = tempPartDict

		print(len(customerDict))
		print(len(listPriceDict))
		print(len(partnerDict))
		# starting sample code here
		

		customerDict_multiuser = {}

		customerList  = getCustomers_multiuser(customerDict_multiuser)
		print("Got the mulitpartner customerList")
		dateList = getDates()
		print("Got the multipartner dateList")

		partnerDict_multiuser = {}

		getPartnerDict_multiuser(customerList,customerDict_multiuser, partnerDict_multiuser)
		print(len(customerDict_multiuser))
		print("Got the partner_multi dict")

		resultDf_multiuser = pd.DataFrame()
		resultDf_multiuser.to_pickle(string_pickle_filename)
		getRecords_multiuser(dateList, resultDf_multiuser,customerDict_multiuser, partnerDict_multiuser)
		print("Got records multiuser")
		#print(resultDf_multiuser.shape)
		resultDf_multiuser = pd.read_pickle(string_pickle_filename)
		print(len(resultDf_multiuser))
		#csv_temp_multi = json_normalize(json.loads(resultDf_multiuser))

		getListPrice()
		print("Got the list price ")
		
		resultDf_multiuser['ResourceGroup'] = resultDf_multiuser.apply(lambda row: extractResGrp(str(row['instanceData.resourceUri'])), axis=1)
		resultDf_multiuser['ResourceTypeSpecification'] = resultDf_multiuser.apply(lambda row: extractResType(str(row['instanceData.resourceUri'])), axis=1)
		resultDf_multiuser['ListPrice'] = resultDf_multiuser.apply(lambda row: mergePriceList(str(row['resource.id']), row['quantity']), axis=1)

		print("DOne processing")
		tags_cols = [col for col in resultDf_multiuser.columns if 'instanceData.tags' in col]

		resultDf_multiuser['Tags_'] = ""
		#dfForTags = pd.DataFrame()
		for item in tags_cols:
			#a_temp = resultDf['Tags_'].map(str)
			#a_temp =  
			resultDf_multiuser["Tags_"] = resultDf_multiuser['Tags_'].map(str) + resultDf_multiuser[item].map(str)

		resultDf_multiuser['TAGS'] = resultDf_multiuser.apply(lambda row: extractTags_(str(row['Tags_'])), axis=1)



		print("Added Tags")
		
		columnsList = ["customerId","customerCompanyName","subscriptionId","instanceData.location",
		"resource.id","resource.region","resource.name","ResourceGroup","resource.category","resource.subcategory",
		"ResourceTypeSpecification","quantity","unit","ListPrice","offerName","usageStartTime","usageEndTime",
		"domain","billingType","billingCycle","TAGS"]
		
		finaldf_multiuser=  resultDf_multiuser[columnsList]
		
		try:
			finaldf_multiuser = finaldf_multiuser[finaldf_multiuser.ListPrice != "NA"]
		except:
			pass
		finaldf_multiuser = finaldf_multiuser.drop_duplicates(subset = columnsList)

		print("Removed Duplicates")

		
		mycsv = finaldf_multiuser.to_csv()

		return Response( mycsv ,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=CSP_BILL_multi.csv"})


		return Response(json.dumps(customerDict_multiuser),  mimetype='application/json')

		#ending sample code here


		#return session.get('accesToken', None)
		#return Response(json.dumps(getCustomers()),  mimetype='application/json')
		#customerList  = getCustomersTemp(customerDictTemp)
		
		#return Response(json.dumps(customerDictTemp),  mimetype='application/json')

		customerList  = getCustomers()
		#return Response(json.dumps(customerDict),  mimetype='application/json')

		print("Got the customerList")
		dateList = getDates()
		print("Got the dateList")

		partnerDictTemp = {}
		getPartnerDict(customerList)
		#getPartnerDictTemp(customerList)
		print("Got the partnerDict")
		#return "GOT it"
		#return Response(json.dumps(partnerDict),  mimetype='application/json')
		useRecordUrls = getRecordsUrls()
		print("Got the record urls")

		#getRecords(useRecordUrls)
		

		#return "yes got it"

		print(len(resultDf))
		getRecordsTemp(useRecordUrls,dateList)
		print("Got the records")
		print(resultDf.shape)
		print(len(resultDf))

		getListPrice()
		print("Got the list price ")
		#return Response(json.dumps(listPriceDict),  mimetype='application/json')
		#Jsonrecords = json.loads(allrecords)
		#return allrecords
		#return Response(json.dumps(partnerDict),  mimetype='application/json')
		#return Response(json.dumps(allrecords),  mimetype='application/json')
		#JsonrecordsData = json.loads(resultDf.to_json())
		#return Response(json.dumps(JsonrecordsData),  mimetype='application/json')

		#partnerDict = getSubscriptions(customerList)
		#return Response(json.dumps(partnerDict),  mimetype='application/json')

		#dates = pd.date_range('20130101', periods=6)
		#df = pd.DataFrame(np.random.randn(6,4), columns=list('ABCD'))
		#json_data = df.to_json(orient='values')
		#mycsv = df.to_csv()
		'''columnsList = ["attributes.objectType","instanceData.location",
		"instanceData.resourceUri","quantity","resource.category","resource.id","resource.name",
		"resource.region","resource.subcategory","unit","usageEndTime","usageStartTime",
		"customerId","customerCompanyName","domain","billingType","billingCycle","offerName","subscriptionId"]'''
		resultDf['ResourceGroup'] = resultDf.apply(lambda row: extractResGrp(str(row['instanceData.resourceUri'])), axis=1)
		resultDf['ResourceTypeSpecification'] = resultDf.apply(lambda row: extractResType(str(row['instanceData.resourceUri'])), axis=1)
		resultDf['ListPrice'] = resultDf.apply(lambda row: mergePriceList(str(row['resource.id']), row['quantity']), axis=1)

		print("DOne processing")
		tags_cols = [col for col in resultDf.columns if 'instanceData.tags' in col]
		#resultDf['TagsTemp']  = resultDf[tags_cols].values.tolist()
		#resultDf['Tags'] = resultDf.apply(lambda row: extractTags(str(row['TagsTemp'])), axis=1)
		#resultDf['Tags'] = ""
		'''for item in tags_cols:
			resultDf['Tags'] = resultDf['Tags'] + resultDf.apply(lambda row: extractTags(str(row[item])), axis=1)

		'''
		resultDf['Tags_'] = ""
		#dfForTags = pd.DataFrame()
		for item in tags_cols:
			#a_temp = resultDf['Tags_'].map(str)
			#a_temp =  
			resultDf["Tags_"] = resultDf['Tags_'].map(str) + resultDf[item].map(str)

		resultDf['TAGS'] = resultDf.apply(lambda row: extractTags_(str(row['Tags_'])), axis=1)



		print("Added Tags")
		#return Response(json.dumps(tags_cols),  mimetype='application/json')

		columnsList = ["customerId","customerCompanyName","subscriptionId","instanceData.location",
		"resource.id","resource.region","resource.name","ResourceGroup","resource.category","resource.subcategory",
		"ResourceTypeSpecification","quantity","unit","ListPrice","offerName","usageStartTime","usageEndTime",
		"domain","billingType","billingCycle","TAGS"]
		'''columnsList = ["customerId","customerCompanyName","subscriptionId","instanceData.location",
		"resource.id","resource.region","resource.name","ResourceGroup","resource.category","resource.subcategory",
		"ResourceTypeSpecification","quantity","unit","offerName","usageStartTime","usageEndTime",
		"domain","billingType","billingCycle"]'''

		finaldf=  resultDf[columnsList]
		
		try:
			finaldf = finaldf[finaldf.ListPrice != "NA"]
		except:
			pass
		finaldf = finaldf.drop_duplicates(subset = columnsList)

		print("Removed Duplicates")

		#tempDf = resultDf.filter(regex = 'instanceData.tags')
		
		#mycsv = tempDf.to_csv()

		#for index, row in finaldf:
			#resourceURI = row["instanceData.resourceUri"]

		#mycsv = resultDf.to_csv()
		#finaldf['ResourceGroup'] = finaldf.apply(lambda row: extractResGrp(str(row['instanceData.resourceUri'])), axis=1)
		#finaldf['ResourceTypeSpecification'] = finaldf.apply(lambda row: extractResType(str(row['instanceData.resourceUri'])), axis=1)
		mycsv = finaldf.to_csv()

		#r =  make_response(render_template('afterRequest.html'))
		return Response( mycsv ,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=CSP_BILL.csv"})


if __name__ == '__main__':
	app.run( threaded= True)