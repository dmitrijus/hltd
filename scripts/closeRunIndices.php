<?php 

function jsonDecode($json) 
{  
    // Author: walidator.info 2009 
    $comment = false; 
    $out = '$x='; 

    for ($i=0; $i<strlen($json); $i++) 
    { 
        if (!$comment) 
        { 
            if ($json[$i] == '{' || $json[$i] == '[')        $out .= ' array('; 
            else if ($json[$i] == '}' || $json[$i] == ']')    $out .= ')'; 
            else if ($json[$i] == ':')    $out .= '=>'; 
            else                         $out .= $json[$i];            
        } 
        else $out .= $json[$i]; 
        if ($json[$i] == '"')    $comment = !$comment; 
    } 
    eval($out . ';'); 
    return $x; 
};

$tribename=$argv[1];
$run=$argv[2];

date_default_timezone_set("Europe/Zurich");
$now = new DateTime();
echo $now->format('Y-m-d H:i:s')."\n";
echo "Closing run ".$run." in tribe cluster ".$tribename."\n";


$crl = curl_init();
$url = 'http://'.$argv[1].':9200/_nodes/bu*/_none';
curl_setopt ($crl, CURLOPT_URL,$url);
curl_setopt ($crl, CURLOPT_RETURNTRANSFER, 1);

$ret = curl_exec($crl);
$res=json_decode($ret,true);
$what=$res["nodes"];

$crlm = curl_multi_init();
$curl_arr = array();
$node_count = count($what);
//echo $what;

$host_list = array();

$i=0;
foreach ($what as $key => $value){

//for($i = 0; $i < $node_count; $i++) {

	$appliance =  $value["host"];
	$host_list[$i]=$appliance;

        $applianceName = $value["host"];
	$applianceName = preg_replace('/.cern.ch$/', '', $applianceName);
	$applianceName = preg_replace('/.cms$/', '', $applianceName);

	//echo $value["host"];
	//$url = 'http://'.$value["host"].':9200/run'.$run.'_appliance_'.$value["host"].'/_close';
	$url = 'http://'.$appliance.':9200/run'.$run.'_appliance_'.$applianceName.'/_close';

	$data='';
	$curl_arr[$i] = curl_init();
        curl_setopt ($curl_arr[$i], CURLOPT_URL,$url);
        curl_setopt ($curl_arr[$i], CURLOPT_RETURNTRANSFER, 1);
        curl_setopt ($curl_arr[$i], CURLOPT_POSTFIELDS, $data);
	//$ret=curl_exec($crl);
	curl_multi_add_handle($crlm,$curl_arr[$i]);

	$i++;

        //echo "POST ".$url." executed\n";	
 
}
do {
	$mrc = curl_multi_exec($crlm,$running);
	$sres = curl_multi_select($crlm);
	if ($res == false) {}
} while  ($running > 0);

$repeat_list = array();
$repeat_pos=0;


function checkStatus($curl_arr_,&$node_count_,&$repeat_list_,&$repeat_pos_,&$host_list_,$is_repeat) {
  $mExceptionStr = "IndexMissingException";
  $length = strlen($mExceptionStr);

  for($i = 0; $i < $node_count_; $i++) {
	$retvalraw = curl_multi_getcontent( $curl_arr_[$i] );
	$retval = jsonDecode($retvalraw);
	if (array_key_exists ("acknowledged", $retval )) {
		if ($retval["acknowledged"]==false) {
			if ($is_repeat)
				echo $repeat_list_[$i].":acknowledged:false\n";
			else
			  	echo $host_list_[$i].":acknowledged:false\n";
			$repeat_list_[$repeat_pos_]=$host_list_[$i];
			$repeat_pos++;
		}
	  if ($retval["acknowledged"]==true) {}//echo "true!\n";
	}
	else if (array_key_exists ("status", $retval)) {
		if ($retval["status"]==404) {
			if (substr($retval["error"], 0, $length) == $mExceptionStr) {}//{echo "ok\n";}
			else {
			  if ($is_repeat)
				  echo $repeat_list_[$i].":not closed? server response: ".$retvalraw."\n";
			  else
				  echo $host_list_[$i].":not closed? server response: ".$retvalraw."\n";

			  $repeat_list_[$repeat_pos_]=$host_list_[$i];
			  $repeat_pos_++;
			}

		}
	}
  }
}


checkStatus($curl_arr,$node_count,$repeat_list,$repeat_pos,$host_list,false);


$crlm2 = curl_multi_init();
$curl_arr2 = array();
$node_count2 = count($repeat_list);
$i=0;

if ($node_count2==0) return 0;

echo "retries..".$node_count2."\n";

foreach ($repeat_list as $key => $value){

	$appliance =  $value;
	$url = 'http://'.$appliance.':9200/run'.$run.'_appliance_'.$appliance.'/_close';
	$data='';
	$curl_arr2[$i] = curl_init();
        curl_setopt ($curl_arr2[$i], CURLOPT_URL,$url);
        curl_setopt ($curl_arr2[$i], CURLOPT_RETURNTRANSFER, 1);
        curl_setopt ($curl_arr2[$i], CURLOPT_POSTFIELDS, $data);
	//$ret=curl_exec($crl);
	curl_multi_add_handle($crlm2,$curl_arr2[$i]);
	$i++;
        //echo "POST ".$url." retry executed\n";	
}

do {
	$mrc = curl_multi_exec($crlm2,$running);
	$sres = curl_multi_select($crlm2);
	if ($res == false) {}
} while  ($running > 0);

$repeat_pos=0;
checkStatus($curl_arr2,$node_count2,$repeat_list,$repeat_pos,$repeat_list,true);

return 0;
?>
