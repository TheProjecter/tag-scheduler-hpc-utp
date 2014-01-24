#!/usr/bin/python
#ref: http://lxml.de/objectify.html
#from objectifiedJson import objectJSONEncoder
from lxml import objectify

#useful constants
WR_STATE_EMPTY = -1
WR_STATE_PRESENT = -2

f = open("xs_input.xml", "r")
text = f.read()

xsInput = objectify.fromstring(text)


print "Shared memory state: ", xsInput.project.shared_mem.ready
print "Shared memory size: ", xsInput.project.shared_mem.ss_size


print "Len: ", len(xsInput.project.shared_mem.platforms.platform)

for platform in xsInput.project.shared_mem.platforms.platform:
    print "Platform name: ", platform.name

print "Work units:"
for wu_result in xsInput.project.shared_mem.wu_results.wu_result:
    print "\tState: ", wu_result.state
    print "\tWU : ", wu_result.workunit.name
    print "\tEst. FOPS: ", wu_result.workunit.rsc_fpops_est
    print "\n"
    
#TODO: grab list of app_versions, assignment, wu_results