#!/usr/bin/python
"""
ref: http://lxml.de/objectify.html

Use round robin, FIFO scheme. 1 unit assigned per round
"""
#from objectifiedJson import objectJSONEncoder
from lxml import objectify
from lxml import etree

#class definitions
class Workunit:
    def __init__(self):
        pass
    
class WorkunitResult:
    def __init__(self):
        self.workunit = Workunit()
        
    def __repr__(self):
        return '{}: {} {} {}'.format(self.__class__.__name__, self.id, self.workunit.name, self.workunit.id)            
   
class Request:
    def __init__(self):
        self.reliability = float(0)
        self.workAssigned = float(0)
        
    def __repr__(self):
        return '{}: {} {}'.format(self.__class__.__name__, self.hostid, self.reliability)
    
    """    
    def __cmp__(self, other):
        if hasattr(other, 'reliability'):
            return self.reliability.__cmp__(other.reliability) 
    """
    #comparison methods
    def __eq__(self, other):
        return self.reliability == other.reliability
    def __ne__(self, other):
        return self.reliability != other.reliability
    def __lt__(self, other):
        return self.reliability < other.reliability
    def __gt__(self, other):
        return self.reliability > other.reliability
    def __le__(self, other):
        return self.reliability <= other.reliability
    def __ge__(self, other):
        return self.reliability >= other.reliability
            
class HostAvailability:
    def __init__(self, year, month, day, hour, minute, availability):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.availability = availability
    
class Assignment:
    def __init__(self, hostId, wuId, resultId):
        self.hostId = hostId
        self.wuId = wuId
        self.resultId = resultId
        
    def __repr__(self):
        return '{}: H{} WU{} R{}'.format(self.__class__.__name__, self.hostid, self.wuId, self.resultId)        
            
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

def readSchedulerInput(filename):
    inFile = open(filename, "r")
    text = inFile.read()
    inFile.close()  
    return objectify.fromstring(text)


"""
print "Shared memory state: ", xsInput.project.shared_mem.ready
print "Shared memory size: ", xsInput.project.shared_mem.ss_size

print "Len: ", len(xsInput.project.shared_mem.platforms.platform)

for platform in xsInput.project.shared_mem.platforms.platform:
    print "Platform name: ", platform.name
"""

def readWorkunitResults(xsInput):
    #Results
    wuResults = list()
    #nWUResults = len(xsInput.project.shared_mem.wu_results.wu_result)
    totalWorkAvailable = 0
    print "Work units:"
    for wuResult in xsInput.project.shared_mem.wu_results.wu_result:
        
        wuR = WorkunitResult()
        
        wuR.state = wuResult.state
        wuR.id = wuResult.resultid
        wuR.workunit.id = wuResult.workunit.id
        wuR.workunit.name = wuResult.workunit.name
        wuR.workunit.estFpops = wuResult.workunit.rsc_fpops_est
        totalWorkAvailable += wuResult.workunit.rsc_fpops_est
        wuResults.append(wuR)
        
    print "Is there work available: ", "Yes" if (totalWorkAvailable > 0 ) else "No"
    print "Workunit results available: ", len(wuResults)
    
    return wuResults
    
#for wuResult in wuResults:    
#    print "R Id: ", wuResult.id
#    print "State: ", wuResult.state
#    print "WU Id:", wuResult.workunit.id
#    print "Name: ", wuResult.workunit.name
#    print "Fpops: ", wuResult.workunit.estFpops

def readRequests(xsInput):
    #Requests
    requests = list()
    #nRequests = len(xsInput.clients.requests.scheduler_request)
    
    for schedulerRequest in xsInput.clients.requests.scheduler_request:
        
        request = Request()
        
        request.hostid = schedulerRequest.hostid
        request.workRequested = schedulerRequest.work_req_seconds
        request.availabilityRecords = list()
        
        for record in schedulerRequest.host_availability.record:
            
            availabilityRecord = HostAvailability(record.date.year, record.date.month, record.date.day,
                                                  record.time.hour, record.time.minute,
                                                  record.availability)
            """        
            availabilityRecord.hour = record.time.hour
            availabilityRecord.minute = record.time.minute
            availabilityRecord.year = record.date.year
            availabilityRecord.month = record.date.month
            availabilityRecord.day = record.date.day
            availabilityRecord.availability = record.availability
            """
            
            #print "Year ", availabilityRecord.year, " Month ", availabilityRecord.month, " Day ", availabilityRecord.day, " Hour ", availabilityRecord.hour, " Minute ", availabilityRecord.minute, " Availability ", availabilityRecord.availability
            
            request.availabilityRecords.append(availabilityRecord)
            
        request.reliability = computeHostReliability(request.availabilityRecords)
            
        requests.append(request)
        
    print "Total requests: ", len(requests)
    return requests
  
#Write assignment

"""
        <host_id>3</host_id>
        <wu_id>2</wu_id>
        <result_id>5</result_id>
"""


#Perform assignments
""" 
TODO: grab list of app_versions, assignment, wu_results, nodes
sort lists
    nodes: highest demand to lowest for work + availability
    wu_results: highest fpops to lowest
"""

#compute host reliability
def computeHostReliability(availabilityRecords):
    
    #collect last 30 records
    availableCount = 0
    
    for availabilityRecord in availabilityRecords:
        if availabilityRecord.availability == 1:
            availableCount += 1
            
    reliability = float(availableCount)/float(len(availabilityRecords))
    
    print "Count, Reliability, Total: ", availableCount, reliability, len(availabilityRecords)
    
    return reliability
    
    #return reliability score

#assign work (round robin, raking/sorting: wants work, is available, FIFO
def scheduleWork(wuResults, requests):
  
    #check if there is work to be processed
    totalWorkAvailable = 0
    for wuResult in wuResults:
        totalWorkAvailable += wuResult.workunit.estFpops
    
    if totalWorkAvailable == 0:
        return list()
    
    #form list of hosts requesting work
    hostsRequestingWork = list()
    
    for request in requests:
        if request.workRequested > 0:
            hostsRequestingWork.append(request)
    
    if len(hostsRequestingWork) == 0:
        return list()
    
    print(hostsRequestingWork)
    
    #sort hosts by availability, in decreasing order
    sortedHosts = sorted(hostsRequestingWork, reverse=True)
    
    print(sortedHosts)
    
    #start round robin assignment
    return roundRobin(wuResults, sortedHosts) 

def roundRobin(wuResults, hosts):
    
    #requestIndex = 0
    #requestLimitIndex = len(hosts) - 1
    print wuResults
    #print hosts
    
    assignments = list()   
    
    #TODO: check work unit state
    for wuResult in wuResults:
        hostIndex = 0
        assigned = False
         
        for host in hosts:
            
            #print "Estimate ", host.workAssigned + wuResult.workunit.estFpops, host.workRequested            
                        
            if (host.workAssigned + wuResult.workunit.estFpops) <= host.workRequested:    
                print "Bingo"            
                assignment = Assignment(host.hostid, wuResult.workunit.id, wuResult.id)
                assignments.append(assignment)
                print "Assigning ", assignment
                host.workAssigned += wuResult.workunit.estFpops
                assigned = True
                break
            #increment host index
            hostIndex += 1
        
        if assigned == True:
            #move host that was assigned work to the bottom of the list
            hosts.insert(len(hosts) - 1, hosts.pop(hostIndex))        
    
    return assignments
    
def writeSchedulerOutput(filename, assignments): 
    
    print "Assignments: ", assignments   

    root = etree.Element("xs_output")
  
    for assignment in assignments:
            
        assignmentElement = etree.SubElement(root, "assignment")
        hostIdElement = etree.SubElement(assignmentElement, "host_id")
        wuIdElement = etree.SubElement(assignmentElement, "wu_id")
        resultIdElement = etree.SubElement(assignmentElement, "result_id")
        hostIdElement.text = str(assignment.hostId)
        wuIdElement.text = str(assignment.wuId)
        resultIdElement.text = str(assignment.resultId)    

    outFile = open(filename, "w")
    
    if(len(assignments) > 0):        
        outFile.write(etree.tostring(root, pretty_print=True))
    else:
        outFile.write("<xs_output>")
        outFile.write("<xs_output/>")
            
    outFile.close()
    print(etree.tostring(root, pretty_print=True))
    

def main():
    xsInput = readSchedulerInput("xs_input.xml")
    wuResults = readWorkunitResults(xsInput)
    requests = readRequests(xsInput)
    assignments = scheduleWork(wuResults, requests)
    writeSchedulerOutput("xs_output.xml", assignments)

    
main()
#roundrobin()
    