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
    maxStrSize = 130000
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
    if command.command == 'ssh':
        sshQ.put({'command':command.data['command']})
        
    elif command.command == 'configFile' and command.data['topic'] == 'addFile':
         newUsrVar(command.data['content'])
         
    else: print command.timestamp.isoformat() + ': ' + command.command + ' ---> unknown'      # do nothing

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
            print 'try to send event: ssh'
            client.publishEvent('ssh', "json", {'d':{'answer':answer}})
            sshQ.task_done()

def checkVGdat(client):
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
            client.publishEvent('heartbeat', "json", {'d':[{'name':'vgdat', 'state':str(A), 'comment':status}]})
        else:
            print 'no eBusLog.vgdat in ' + filePath

def vgdatSender(client):
    filePath = '/home/pi/Data/Rawdata/'
    #filePath = 'C:\\Users\\ceidam\\Eigene Dateien\\fieldtest monitoring\\packageSender\\'
    while True:
        time.sleep(300)
        fileList = os.listdir(filePath)
        clearedList = [ x for x in fileList if "eBusLog" in x and ".vgdat" in x ]
        if len(clearedList)>0:
            for fileName in clearedList:
                dateStr = fileName[len(fileName)-19:-13]
                date = '20'+dateStr[-2:]+'-'+dateStr[2:4]+'-'+dateStr[0:2]
                filename, cmpstrList = file_zipper(filePath+fileName)
                for index in range(0,len(cmpstrList)):
                    print 'filename:'+filename + ' | index:'+str(index)
                    mqtt.MQTTpostEvent('rawData.vgdat', {'date':date, 'filename':filename, 'index':str(index), 'content':cmpstrList[index]}, client, 'externalDevice', deviceId)
                    time.sleep(2)
                os.remove(filePath+fileName)
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
    deviceId = ''.join(subprocess.check_output('cat /sys/class/net/eth0/address', shell=True)).replace(':','').replace('\n','')
    #deviceId = '333'
    print 'my deviceId: ' + deviceId
    
    ### mqtt client and callbacks ##########################################
    client = mqtt.MQTTconnect(deviceId)
    mqtt.MQTTget(myCommandCallback,client)
    
    ### queues #############################################################
    sshQ = queue.Queue(maxsize=0)
    
    ### threads ############################################################
    SSHinteractionT = threading.Thread(target=SSHinteraction, args=(sshQ,client,), name='SSHinteractionT')
    SSHinteractionT.daemon = True
    SSHinteractionT.start()
    
    checkVGdatT = threading.Thread(target=checkVGdat, args=(client,), name='checkVGdatT')
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
















