#!/usr/bin/python
"""
Thermal Aware Grid (TAG) project. 
@summary: Simple scheduler that measures hosts reliability to rank
hosts for work assignment. (FYP project)

Use round robin, FIFO scheme.

@author: Guilherme Dinis Jr. (guilherme.dinisjunior@gmail.com)
@organization: Universiti Teknologi PETRONAS, High Performance Computing Center

ref: http://lxml.de/objectify.html
"""
#from objectifiedJson import objectJSONEncoder
from lxml import objectify
from lxml import etree

"""
Data structures definitions
"""

#Holds details of workunit entities from BOINC
class Workunit:
    def __init__(self, name, wuId, fpops):
        self.name = name
        self.id = wuId
        self.fpops = fpops

#Holds details of specific results from workunits in BOINC
class WorkunitResult:
    def __init__(self, resultId, workunit):
        self.id = resultId
        self.workunit = workunit
        
    def __repr__(self):
        return '{}: {} {} {}'.format(self.__class__.__name__, self.id, self.workunit.name, self.workunit.id)            

#Holds details of a particular host making a request
class Host:
    def __init__(self, hostid, fpops):
        self.id = hostid
        self.fpops = fpops
        self.reliability = float(0)

#Holds details of a work request sent by a host
class Request:
    def __init__(self, host):
        self.host = host
        self.workAssigned = float(0)

    def __repr__(self):
        return '{}: {} {}'.format(self.__class__.__name__, self.host.id, self.host.reliability)
    
    """    
    def __cmp__(self, other):
        if hasattr(other, 'reliability'):
            return self.reliability.__cmp__(other.reliability) 
    """
    #comparison methods
    def __eq__(self, other):
        return self.host.reliability == other.host.reliability
    def __ne__(self, other):
        return self.host.reliability != other.host.reliability
    def __lt__(self, other):
        return self.host.reliability < other.host.reliability
    def __gt__(self, other):
        return self.host.reliability > other.host.reliability
    def __le__(self, other):
        return self.host.reliability <= other.host.reliability
    def __ge__(self, other):
        return self.host.reliability >= other.host.reliability
    
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
       
#Holds details of a thermal availability log from a host
class HostAvailability:
    def __init__(self, year, month, day, hour, minute, availability):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.availability = availability

#Holds details of a work assignment done by the scheduler for a particular host
class Assignment:
    def __init__(self, hostId, wuId, resultId):
        self.hostId = hostId
        self.wuId = wuId
        self.resultId = resultId
        
    def __repr__(self):
        return '{}: H{} WU{} R{}'.format(self.__class__.__name__, self.hostId, self.wuId, self.resultId)        
            
"""
Useful constatns
"""
WR_STATE_EMPTY = -1
WR_STATE_PRESENT = -2

"""
Functions.Methods.Actions
"""

#Reads the input file provided by BOINC XSM and returns it into a Python object
def readSchedulerInput(filename):
    inFile = open(filename, "r")
    text = inFile.read()
    inFile.close()  
    return objectify.fromstring(text)


#Read results (waiting for assignment) from XSM
def readWorkunitResults(xsInput):
    
    wuResults = list()
    
    totalWorkAvailable = 0
    print "Work units:"
    for wuResult in xsInput.project.shared_mem.wu_results.wu_result:
        
        #get results that are ready to be sent out only
        if wuResult.state == WR_STATE_PRESENT:   
                 
            wuR = WorkunitResult(wuResult.resultid,                            
                                 Workunit(wuResult.workunit.name, 
                                          wuResult.workunit.id,
                                          wuResult.workunit.rsc_fpops_est))

            totalWorkAvailable += wuResult.workunit.rsc_fpops_est
            wuResults.append(wuR)
            
    print "Is there work available: ", "Yes" if (totalWorkAvailable > 0 ) else "No"
    print "Workunit results available: ", len(wuResults)
    
    return wuResults
    
#reads requests from Python object and returns a list
def readRequests(xsInput):
   
    requests = list()
       
    for schedulerRequest in xsInput.clients.requests.scheduler_request:
        
        request = Request(Host(schedulerRequest.hostid, schedulerRequest.host_info.p_fpops))
        
        request.workRequested = schedulerRequest.work_req_seconds
        request.availabilityRecords = list()
                
        for record in schedulerRequest.host_availability.record:
            
            availabilityRecord = HostAvailability(record.date.year, record.date.month, record.date.day,
                                                  record.time.hour, record.time.minute,
                                                  record.availability)
           
            #print "Year ", availabilityRecord.year, " Month ", availabilityRecord.month, " Day ", availabilityRecord.day, " Hour ", availabilityRecord.hour, " Minute ", availabilityRecord.minute, " Availability ", availabilityRecord.availability            
            request.availabilityRecords.append(availabilityRecord)
            
        request.host.reliability = computeHostReliability(request.availabilityRecords)            
        requests.append(request)
        
    print "Total requests: ", len(requests)
    return requests
  
#Write assignment

"""
        <host_id>3</host_id>
        <wu_id>2</wu_id>
        <result_id>5</result_id>
"""


#Compute host reliability based on thermal records
def computeHostReliability(availabilityRecords):
    
    """
    TODO: get records for the last half-hour only
    """

    availableCount = 0
    
    for availabilityRecord in availabilityRecords:
        if availabilityRecord.availability == 1:
            availableCount += 1
            
    reliability = float(availableCount)/float(len(availabilityRecords))
    
    print "Count, Reliability, Total: ", availableCount, reliability, len(availabilityRecords)
    
    return reliability
    
    #return reliability score

#Assign work (round robin, raking/sorting: wants work, is available, FIFO) to 
def scheduleWork(wuResults, requests):
  
    #check if there is work to be processed
    totalWorkAvailable = 0
    for wuResult in wuResults:
        totalWorkAvailable += wuResult.workunit.fpops
    
    if totalWorkAvailable == 0:
        return list()
    
    #form list of hosts requesting work
    requestForWork = list()
    
    for request in requests:
        if request.workRequested > 0:
            requestForWork.append(request)
    
    if len(requestForWork) == 0:
        return list()
    
    #sort hosts by availability, in decreasing order
    sortedWorkRequests = sorted(requestForWork, reverse=True)
    #start round robin assignment
    return roundRobin(wuResults, sortedWorkRequests) 

#Assign work to nodes using a round-robin scheme
def roundRobin(wuResults, requests):
        
    assignments = list()   
    
    for wuResult in wuResults:
        hostIndex = 0
        assigned = False
         
        for request in requests:       
                        
            if (request.workAssigned + (wuResult.workunit.fpops/request.host.fpops)) <= request.workRequested:    
                            
                assignment = Assignment(request.host.id, wuResult.workunit.id, wuResult.id)
                assignments.append(assignment)
                print assignment
                request.workAssigned += wuResult.workunit.fpops/request.host.fpops
                assigned = True
                break
            
            #increment host index
            hostIndex += 1
        
        if assigned == True:
            #move host that was assigned work to the bottom of the list
            requests.insert(len(requests) - 1, requests.pop(hostIndex))     
            
    print "Total assignments: ", len(assignments), " out of ", len(wuResults)   
    
    print "Work assignments:"
    for request in requests:
        print "[HOST: ", request.host.id, "] [WORK ASSIGNED: ", request.workAssigned, "]"
    
    return assignments
    
#Create XML file with work assignment for BOINC's XSM
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
    
    return
    

#Program's entry point
def main():
    
    """
    TODO: file input and output as parameters
    """    
    
    xsInput = readSchedulerInput("xs_input.xml")
    wuResults = readWorkunitResults(xsInput)
    requests = readRequests(xsInput)
    assignments = scheduleWork(wuResults, requests)
    writeSchedulerOutput("xs_output.xml", assignments)

#Program execution call. 
main()

    