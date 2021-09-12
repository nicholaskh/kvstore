/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#include "phxpaxos/node.h"
#include "pnode.h"

namespace phxpaxos
{

int Node :: RunNode(const Options & oOptions, Node *& poNode , LogStorage * &pStorage,phxkv::PhxKVSM& kvsm)
{
    if (oOptions.bIsLargeValueMode)
    {
        InsideOptions::Instance()->SetAsLargeBufferMode();
    }
    
    InsideOptions::Instance()->SetGroupCount(oOptions.iGroupCount);
        
    poNode = nullptr;
    NetWork * poNetWork = nullptr;

    Breakpoint::m_poBreakpoint = nullptr;
    BP->SetInstance(oOptions.poBreakpoint);

    PNode * poRealNode = new PNode();
    
    int ret = poRealNode->Init(oOptions, poNetWork,pStorage,kvsm);
    if (ret != 0)
    {
        ERROR_LOG("poRealNode->Init error");
        delete poRealNode;
        return ret;
    }

    //step1 set node to network
    //very important, let network on recieve callback can work.
    poNetWork->m_poNode = poRealNode;

    //step2 run network.
    //start recieve message from network, so all must init before this step.
    //must be the last step.
    poNetWork->RunNetWork();
    poNode = poRealNode;
    INFO_LOG("RunNode ok..");
    return 0;
}
    
}


