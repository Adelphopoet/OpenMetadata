/*
 *  Copyright 2022 Collate.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import antlr4 from 'antlr4';
import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import './styles/index';
import { getBasePath } from './utils/HistoryUtils';

const patchAntlrDeserializer = () => {
  const deserializer = antlr4?.atn?.ATNDeserializer;
  const proto = deserializer?.prototype as
    | (typeof antlr4.atn.ATNDeserializer.prototype & {
        __omPatched?: boolean;
      })
    | undefined;

  if (!proto || proto.__omPatched) {
    return;
  }

  const originalReset = proto.reset;

  const toLegacyString = (data: ArrayLike<number>) => {
    let out = '';
    const chunkSize = 16384;
    for (let i = 0; i < data.length; i += chunkSize) {
      const chunk = Array.prototype.slice.call(data, i, i + chunkSize);
      out += String.fromCharCode.apply(null, chunk);
    }

    return out;
  };

  proto.reset = function reset(data: unknown) {
    if (
      data &&
      typeof data !== 'string' &&
      (Array.isArray(data) || ArrayBuffer.isView(data))
    ) {
      const first = (data as ArrayLike<number>)[0];
      if (first === 3) {
        data = toLegacyString(data as ArrayLike<number>);
      }
    }

    return originalReset.call(this, data as never);
  };

  proto.__omPatched = true;
};

patchAntlrDeserializer();

const container = document.getElementById('root');
if (!container) {
  throw new Error('Failed to find the root element');
}
const root = createRoot(container);

root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

if ('serviceWorker' in navigator && 'indexedDB' in window) {
  window.addEventListener('load', () => {
    const basePath = getBasePath();
    const serviceWorkerPath = basePath
      ? `${basePath}/app-worker.js`
      : '/app-worker.js';
    navigator.serviceWorker.register(serviceWorkerPath);
  });
}
