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

import java.util.Set;

public class InfeasiblePlanException extends RuntimeException {

    public InfeasiblePlanException() {
    }

    public InfeasiblePlanException(String message, Throwable cause) {
        super(message, cause);
    }

    public InfeasiblePlanException(String message) {
        super(message);
    }

    public InfeasiblePlanException(Throwable cause) {
        super(cause);
    }

    public InfeasiblePlanException(Set<PhaseError> errors) {
        super(makeDescription(errors));
    }

    private static String makeDescription(Set<PhaseError> errors) {
        StringBuilder builder = new StringBuilder();
        for (PhaseError error : errors) {
            if (builder.length()>0) builder.append('\n');
            builder.append(error);
        }
        return builder.toString();
    }

    private static final long serialVersionUID = 1L;

}
