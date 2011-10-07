<?php

/*
 * Copyright (c) 2011  Somia Dynamoid Oy
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

class InsertBuffer {

	const PROTOCOL_VERSION  = 1;

	const SOCKET_PATH       = '/tmp/insertbuffer.socket';

	const TIMEOUT_CONNECT   = 1;
	const TIMEOUT_STREAM    = -1;

	/**
	 * @param  string $query
	 * @param  string $database
	 * @return bool
	 */
	static public function query($query, $database) {
		$request_data = pack('V', strlen($database)) . $database . $query;
		$request_head = pack('VV', self::PROTOCOL_VERSION, strlen($request_data));
		$request = $request_head . $request_data;

		$socket = @fsockopen('unix://' . self::SOCKET_PATH, -1, $errno, $errstr, self::TIMEOUT_CONNECT);
		if ($errno || !is_resource($socket)) {
			error_log('InsertBuffer connect failed: ' . $errstr);
			return false;
		}

		if (self::TIMEOUT_STREAM > 0) {
			stream_set_timeout($socket, self::TIMEOUT_STREAM);
		}

		if (fwrite($socket, $request) !== strlen($request)) {
			error_log('InsertBuffer write failed');
			fclose($socket);
			return false;
		}

		$response_size = 8;
		$response = fread($socket, $response_size);

		fclose($socket);

		if ($response === false) {
			error_log('InsertBuffer read failed');
			return false;
		}

		if (strlen($response) >= 4) {
			$values = unpack('Vversion', substr($response, 0, 4));
			if ($values['version'] != self::PROTOCOL_VERSION) {
				error_log('InsertBuffer response version mismatch');
				return false;
			}
		}

		if (strlen($response) < $response_size) {
			error_log('InsertBuffer response too short');
			return false;
		}

		$values = unpack('Vresult', substr($response, 4, 4));
		if ($values['result'] != 1) {
			error_log('InsertBuffer result unknown');
			return false;
		}

		return true;
	}

}
