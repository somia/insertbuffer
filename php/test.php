<?php

include 'insertbuffer.php';

$database = 'test';

for ($num = 0; $num < 10; $num++) {
	$query = 'INSERT INTO table1 (num) VALUES (' . $num . ')';
	$result = InsertBuffer::query($query, $database);

	echo $query . ' => ';
	var_dump($result);
}
