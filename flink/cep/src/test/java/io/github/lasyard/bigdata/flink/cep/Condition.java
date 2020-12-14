/*
 * Copyright 2020 lasyard@github.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lasyard.bigdata.flink.cep;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;

class Condition extends SimpleCondition<String> {
    private static final long serialVersionUID = 8430169955230929953L;

    private final String str;

    Condition(String str) {
        this.str = str;
    }

    @Override
    public boolean filter(String str) {
        if (str == null) {
            return false;
        }
        return str.startsWith(this.str);
    }
}
