#!/usr/bin/python
"""
ref: http://lxml.de/objectify.html
"""
#from objectifiedJson import objectJSONEncoder
from lxml import objectify

#class definitions
class Workunit:
    def __init__(self):
        pass
    
class WorkunitResult:
    def __init__(self):
        self.workunit = Workunit()
 
class Node:
    def __init__(self):
        pass
    
class Request:
    def __init__(self):
        pass
    
class HostAvailability:
    def __init__(self):
        pass
    
"""
                    <time>
                        <hour>21</hour>
                        <minute>13</minute>
                    </time>
                    <date>
                        <year>2014</year>
                        <month>1</month>
                        <day>16</day>
                    </date>
                    <availability>1.000000</availability>    
"""
    
#useful constants
WR_STATE_EMPTY = -1
WR_STATE_PRESENT = -2

f = open("xs_input.xml", "r")
text = f.read()

xsInput = objectify.fromstring(text)

"""
print "Shared memory state: ", xsInput.project.shared_mem.ready
print "Shared memory size: ", xsInput.project.shared_mem.ss_size

print "Len: ", len(xsInput.project.shared_mem.platforms.platform)

for platform in xsInput.project.shared_mem.platforms.platform:
    print "Platform name: ", platform.name
"""
#Results
wuResults = list()
nWUResults = len(xsInput.project.shared_mem.wu_results.wu_result)

print "Work units:"
for wuResult in xsInput.project.shared_mem.wu_results.wu_result:
    
    wuR = WorkunitResult()
    
    wuR.state = wuResult.state
    wuR.id = wuResult.resultid
    wuR.workunit.id = wuResult.workunit.id
    wuR.workunit.name = wuResult.workunit.name
    wuR.workunit.estFpops = wuResult.workunit.rsc_fpops_est
    wuResults.append(wuR)
    
for wuResult in wuResults:    
    print "R Id: ", wuResult.id
    print "State: ", wuResult.state
    print "WU Id:", wuResult.workunit.id
    print "Name: ", wuResult.workunit.name
    print "Fpops: ", wuResult.workunit.estFpops
    
#Requests
requests = list()
nRequests = len(xsInput.clients.requests.scheduler_request)

for schedulerRequest in xsInput.clients.requests.scheduler_request:
    
    request = Request()
    
    request.hostid = schedulerRequest.hostid
    request.workRequest = schedulerRequest.work_req_seconds
         
   
#TODO: grab list of app_versions, assignment, wu_results, nodes
#sort lists
    #nodes: highest demand to lowest for work + availability
    #wu_results: highest fpops to lowest