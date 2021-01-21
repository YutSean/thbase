"""
Copyright 2021 Yutong Sean

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
__all__ = ['executor', 'handlers', 'type_check', 'check_none']


def type_check(var, t):
    if not isinstance(var, t):
        raise TypeError("A {} object is needed, but got a {}.".format(t.__class, type(var)))


def check_none(var, ms):
    if var is None:
        raise ValueError(ms)


