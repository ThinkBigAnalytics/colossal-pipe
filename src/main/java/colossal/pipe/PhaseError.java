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
package colossal.pipe;

public class PhaseError {
    private String message;
    private Throwable exception;
    
    public PhaseError(String message) {
        this.message = message;
        this.exception = null;
    }
    public PhaseError(Throwable t, String processName) {
        this.message = "Failure running "+processName;
        this.exception = t;
    }
    public String getMessage() {
        return message;
    }
    public Throwable getException() {
        return exception;
    }
}
