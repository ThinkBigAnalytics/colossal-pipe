/*
 * Licensed to Think Big Analytics, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Think Big Analytics, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Copyright 2010 Think Big Analytics. All Rights Reserved.
 */
package colossal.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

import colossal.pipe.PhaseError;

public class EmailAlerter implements Alerter {

    private String[] targetAddresses;
    private String from = System.getProperty("colossal.util.email.alerts.from");
    private String host = System.getProperty("colossal.util.email.alerts.host", "localhost");
    private boolean emailEnabled;
    {
        String defaultAddrs = System.getProperty("colossal.util.email.alerts.to");
        if (defaultAddrs != null) {
            targetAddresses = defaultAddrs.split(",");
            for (int i=0; i<targetAddresses.length; i++)
                targetAddresses[i] = targetAddresses[i].trim();
            emailEnabled = (from != null);
        } else {
            emailEnabled = false;
        }
    }

    @Override
    public void alert(Exception exception, String summary) {
        
        if (!emailEnabled) {
            System.err.println("Alert: "+summary);
            exception.printStackTrace();
            System.err.println("Can't email - not configured");
            return;
        }
        
        System.err.println("Emailing Alert: "+summary);
        exception.printStackTrace();

        try {
            SimpleEmail email = makeFailureEmail();
            StringBuilder msg = new StringBuilder(summary);
            msg.append("Exception:\n");
            StringWriter writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            exception.printStackTrace(printWriter);
            msg.append(writer.toString());
            email.setMsg(msg.toString());
            email.send();
        } catch (EmailException e) {
            System.err.println("Can't send email!");
            e.printStackTrace();
        }
    }

    private SimpleEmail makeFailureEmail() throws EmailException {
        return makeEmail("Job Failure");
    }
    
    private SimpleEmail makeEmail(String title) throws EmailException {
        SimpleEmail email = new SimpleEmail();
        for (String targetAddress : targetAddresses)
            email.addTo(targetAddress);            
        email.setSubject(title);
        email.setFrom(from);
        email.setHostName(host);
        return email;
    }

    @Override
    public void alert(List<PhaseError> errors) {
        System.err.format("%sAlert: can't run pipeline\n", emailEnabled ? "Emailing " : "");
        for (PhaseError e : errors) {
            System.err.println(e.getMessage());
        }
        if (!emailEnabled) {
            System.err.println("Can't email - not configured");
            return;
        }
        
        try {
            SimpleEmail email = makeFailureEmail();
            StringBuilder msg = new StringBuilder();
            for (PhaseError e : errors) {
                msg.append(e.getMessage()).append('\n');
            }
            email.setMsg(msg.toString());
            email.send();
        } catch (EmailException e) {
            System.err.println("Can't send email!");
            e.printStackTrace();
        }
    }

    @Override
    public void pipeCompletion(String pipeName, String summary) {
        System.out.println("Pipe completion: "+pipeName);
        System.out.println(summary);
        if (!emailEnabled) return;
        
        try {
            SimpleEmail email = makeEmail("Pipe completion: "+pipeName);
            email.setMsg(summary);
            email.send();
        }
        catch (EmailException e) {
            System.err.println("Can't send email!");
            e.printStackTrace();
        }
    }

    @Override
    public void alert(String problem) {
        PhaseError err = new PhaseError(problem);
        ArrayList<PhaseError> list = new ArrayList<PhaseError>(1);
        list.add(err);
        alert(list);
    }
    
}
