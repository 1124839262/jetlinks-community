/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.community.plc4x.converter;

import org.apache.plc4x.java.api.value.PlcValue;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Data type converter for PLC4X values to JetLinks property values
 */
public class Plc4xDataConverter {

    /**
     * Convert PLC4X value to Java object
     */
    public static Object toJavaObject(PlcValue plcValue) {
        if (plcValue == null) {
            return null;
        }

        try {
            if (plcValue.isNull()) {
                return null;
            }

            if (plcValue.isList()) {
                return plcValue.getList().stream()
                        .map(Plc4xDataConverter::toJavaObject)
                        .collect(Collectors.toList());
            }

            if (plcValue.isStruct()) {
                Map<String, Object> result = new HashMap<>();
                plcValue.getStruct().forEach((k, v) -> result.put(k, toJavaObject(v)));
                return result;
            }

            return plcValue.getObject();
        } catch (Exception e) {
            return plcValue.toString();
        }
    }
}