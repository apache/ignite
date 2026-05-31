<pre>
<?php

$array = [
    'A','B',
    'C'=>[
        'D','E',
        'F'=>['G','H']
     ],
    'I','J'
    ];
    
$iterator = new RecursiveArrayIterator($array);

/** CHILD_FIRST,SELF_FIRST,LEAVES_ONLY */
$ritit = new RecursiveIteratorIterator($iterator, RecursiveIteratorIterator::SELF_FIRST);

$result = [];
foreach ($ritit as $key=>$value) {
    echo $key,':', $value,'<br>';
    $keys = [];
    foreach (range(0, $ritit->getDepth()) as $depth) {
        $keys[] = $ritit->getSubIterator($depth)->key();
    }
    $result[] = join('.', $keys);
}

var_dump($result);
print("\n");
die;
/**
CHILD_FIRST:
0:A
1:B
0:D
1:E
0:G
1:H
F:Array
C:Array
2:I
3:J
SELF_FIRST:
0:A
1:B
C:Array
0:D
1:E
F:Array
0:G
1:H
2:I
3:J
LEAVES_ONLY:
0:A
1:B
0:D
1:E
0:G
1:H
2:I
3:J
 */
$tree = array();
$tree[1][2][3] = 'lemon';
$tree[1][4] = 'melon';
$tree[2][3] = 'orange';
$tree[2][5] = 'grape';
$tree[3] = 'pineapple';

print_r($tree);
 
$arrayiter = new RecursiveArrayIterator($tree);
$iteriter = new RecursiveIteratorIterator($arrayiter,CHILD_FIRST);
 
foreach ($iteriter as $key => $value) {
  $d = $iteriter->getDepth();
  echo "depth=$d k=$key v=$value\n";
}
?>

