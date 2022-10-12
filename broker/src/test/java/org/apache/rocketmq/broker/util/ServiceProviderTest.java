/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.util;

import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.store.ha.HAService;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ServiceProviderTest {

    @Test
    public void loadTransactionMsgServiceTest() {
        TransactionalMessageService transactionService = ServiceProvider.loadClass(TransactionalMessageService.class);
        assertThat(transactionService).isNotNull();
    }

    @Test
    public void loadAbstractTransactionListenerTest() {
        AbstractTransactionalMessageCheckListener listener = ServiceProvider.loadClass(
                AbstractTransactionalMessageCheckListener.class);
        assertThat(listener).isNotNull();
    }
    
    @Test
    public void loadAccessValidatorTest() {
        List<AccessValidator> accessValidators = ServiceProvider.load(AccessValidator.class);
        assertThat(accessValidators).isNotNull();
        
    }
    
    @Test
    public void loadHAServiceTest() {
        HAService service = ServiceProvider.loadClass(HAService.class);
        assertThat(service).isNotNull();
    }
    
    @Test
    public void loadRPCHookTest() {
    
    }
}
