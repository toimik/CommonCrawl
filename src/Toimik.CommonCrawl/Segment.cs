﻿/*
 * Copyright 2021-2024 nurhafiz@hotmail.sg
 *
 * Licensed under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Toimik.CommonCrawl;

public readonly struct Segment<T>(int index, T value)
{
    /// <summary>
    /// Gets, for this instance, the zero-based index of the <see cref="Value"/> in the segment.
    /// </summary>
    public int Index { get; } = index;

    public T Value { get; } = value;
}