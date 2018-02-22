import os
import time
import Queue as queue
import threading
import subprocess
import zlib
import base64

import mqtt

#------------------------------------------------------------------------------
#                               F U N C T I O N S
#------------------------------------------------------------------------------
def file_zipper(fileName):
    maxStrSize = 100000
    filename = fileName.split('/').pop()
    string = open(fileName, 'r').read()
    bigCmpstr =  base64.b64encode(zlib.compress(string,9))
    cmpstrList = [bigCmpstr[i:i+maxStrSize] for i in range(0, len(bigCmpstr), maxStrSize)]
    print filename + ': original.length: '+str(len(string)) + ' | ' + 'compressed.length: '+str(len(bigCmpstr))
    return filename, cmpstrList

def newUsrVar(content):
    print 'save new uservar'
    filePath = '/home/pi/Data/USERVAR.VG'
    #filePath = "C:\\Users\\ceidam\\Eigene Dateien\\fieldtest monitoring\\packageSender\\USERVAR.VG"
    newfile = open(filePath,"w")
    newfile.write(content)
    newfile.close


#------------------------------------------------------------------------------
#                               C A L L B A C K S
#------------------------------------------------------------------------------
def myCommandCallback(command):
    #print("%-33s%-30s%s" % (command.timestamp.isoformat(), command.device, command.command))
    #print json.dumps(command.data), command.deviceType, command.deviceId
    if command.deviceId == deviceId:
        if command.command == 'ssh':
            sshQ.put({'deviceId':command.deviceId, 'deviceType':command.deviceType, 'command':command.data['command']})
            
        elif command.command == 'configFile' and command.data['topic'] == 'addFile':
             newUsrVar(command.data['content'])
             
        else: print command.timestamp.isoformat() + ': ' + command.command + ' (' +command.data['topic'] + ') unknown'      # do nothing
    else: print command.timestamp.isoformat() + ': ' + command.command + ' not for me!'      # do nothing

def myEventCallback(event):
    print("%-33s%-30s%s" % (event.timestamp.isoformat(), event.device, event.event))
    #print json.dumps(event.data), event.deviceType, event.deviceId

#------------------------------------------------------------------------------
#                               T H R E A D S
#------------------------------------------------------------------------------
def SSHinteraction(sshQ, client):
    while True:
        if not sshQ.empty():
            request = sshQ.get()
            cmdCommand = request['command']
            p = subprocess.Popen(cmdCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            out, err = p.communicate()
            print cmdCommand + ': ' + out+err
            answer = (out + err).decode("ascii", errors="ignore").encode()
            mqtt.MQTTpostEvent('ssh', {'answer':answer}, client, request['deviceType'], request['deviceId'])
            sshQ.task_done()

def checkVGdat(client,deviceId):
    filePath = '/home/pi/Data/'
    #filePath = "C:\\Users\\ceidam\\Eigene Dateien\\fieldtest monitoring\\packageSender\\"
    oldSize=0
    intervall=30
    while True:
        time.sleep(intervall)
        fileList = os.listdir(filePath)
        clearedList = [ x for x in fileList if "eBusLog" in x and ".vgdat" in x ]
        if len(clearedList)>0:
            filename = filePath+clearedList[0]
            newSize=os.path.getsize(filename)
            if newSize>oldSize:
                A=True
                rate=(newSize-oldSize)/intervall
                oldSize=newSize 
            else:
                A=False
                rate=0
            status = 'oldSize:'+str(oldSize) + ' | newSize: '+str(newSize) + ' | state: '+str(A) + ' | rate: '+str(rate)
            print status
            mqtt.MQTTpostEvent('heartbeat', [{'name':'vgdat', 'state':str(A), 'comment':status}], client, 'externalDevice', deviceId)
        else:
            print 'no eBusLog.vgdat in ' + filePath

def vgdatSender(client):
    filePath = '/home/pi/Data/RawData/'
    #filePath = 'C:\\Users\\ceidam\\Eigene Dateien\\fieldtest monitoring\\packageSender\\'
    while True:
        time.sleep(300)
        fileList = os.listdir(filePath)
        clearedList = [ x for x in fileList if "eBusLog" in x and ".vgdat" in x ]
        if len(clearedList)>0:
            for fileName in clearedList:
                filename, cmpstrList = file_zipper(fileName)
                for index in range(0,len(cmpstrList)):
                    print 'filename:'+filename + ' | index:'+str(index)
                    mqtt.MQTTpostEvent('rawData.vgdat', {'filename':filename, 'index':str(index), 'content':cmpstrList[index]}, client, 'externalDevice', deviceId)
                    time.sleep(2)
                os.remove(fileName)
        else:
            print 'no eBusLog.vgdat in ' + filePath

def clockAdjust():
    while True:
        command = '''sudo date -s "$(wget -qSO- --max-redirect=0 google.com 2>&1 | grep Date: | cut -d' ' -f5-8)Z"'''
        subprocess.check_output(command, shell=True)
        print 'clock adjusted'
        time.sleep(24.0*3600)




if __name__ == "__main__":
    
    ### initial ############################################################
    deviceId = ''.join(subprocess.check_output('cat /sys/class/net/eth0/address', shell=True)).replace(':','')
    #deviceId = 'b827eb7e7570'
    print 'my deviceId: ' + deviceId
    
    ### mqtt client and callbacks ##########################################
    client = mqtt.MQTTconnect(deviceId)
    mqtt.MQTTget(myEventCallback,myCommandCallback,client)
    
    ### queues #############################################################
    sshQ = queue.Queue(maxsize=0)
    
    ### threads ############################################################
    SSHinteractionT = threading.Thread(target=SSHinteraction, args=(sshQ,client,), name='SSHinteractionT')
    SSHinteractionT.daemon = True
    SSHinteractionT.start()
    
    checkVGdatT = threading.Thread(target=checkVGdat, args=(client,deviceId,), name='checkVGdatT')
    checkVGdatT.daemon = True
    checkVGdatT.start()
    
    vgdatSenderT = threading.Thread(target=vgdatSender, args=(client,), name='vgdatSenderT')
    vgdatSenderT.daemon = True
    vgdatSenderT.start()
    
    clockerT = threading.Thread(target=clockAdjust, args=(), name='clockerT')
    clockerT.daemon = True
    clockerT.start()
    
    ### bluemix run forever ################################################
    while True:
        time.sleep(1)
















