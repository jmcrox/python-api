# -*- coding: utf8 -*-
'''
    ========================================================================
                                    CygnusCloud
    ========================================================================
    
    File: enums.py    
    Version: 2.0
    Description: enum types syntax extension
    
    © 2012-13 Luis Barrios Hernández, Adrián Fernández Hernández,
        Samuel Guayerbas Martín

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
'''

from functools import partial

def enum(*sequence):
    '''
    A function that generates classes for enum types.
    '''
    values = dict(zip(sequence, range(len(sequence))))
    # Decorate the enum type
    values['toString'] = partial(__toString, values)
    values['fromString'] = partial(__fromString, values)
    values['iterateOverValues'] = partial(__iterateOverValues, values)
    return type('Enum', (), values)

def __iterateOverValues(objectDict, callback):
    for value in objectDict.values():
        if (type(value) is int) :
            callback(value)

def __toString(objectDict, value):
    for key, enumvalue in objectDict.items() :
        if (enumvalue == value):
            return key
    return None

def __fromString(enumType, string_value):
    if string_value in enumType.keys():
        return enumType[string_value]
    else:
        return None
    
if __name__ == "__main__" :
    def dummyCallback(arg):
        print(arg)        
    test = enum("ONE", "TWO", "THREE")
    print(test.ONE)
    print(test.toString(test.ONE))
    print(test.fromString("ONE"))
    test.iterateOverValues(dummyCallback)