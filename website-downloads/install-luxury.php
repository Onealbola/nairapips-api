<?php
// One-time self-contained luxury visual update. No database, credentials, or trading changes.
header('Cache-Control: no-store');
header('X-Content-Type-Options: nosniff');
header('X-Robots-Tag: noindex, nofollow');
session_name('np_luxury_install');
session_start();
function stop_recovery($message) {
 http_response_code(400);
 echo '<h2>Homepage was not changed</h2><p>'.htmlspecialchars($message,ENT_QUOTES,'UTF-8').'</p>';
 exit;
}
function valid_home($body) {
 return is_string($body) && stripos($body,'</body>') !== false
  && strpos($body,'npStandardPlansGrid') !== false && strpos($body,'npPremiumPlansGrid') !== false
  && strpos($body,"location.replace('/admin/") === false;
}
$root = __DIR__;
$target = $root.'/index.html';
if (!is_file($target) || is_link($target)) stop_recovery('Run this file in public_html beside the existing index.html. No files were changed.');
$current = file_get_contents($target);
$source = $target;
$home = $current;
if (!valid_home($home)) stop_recovery('This update needs the existing public homepage. No files were changed.');
$payload = $root.'/.np-luxury-files/luxury-bundle.js';
if (!is_file($payload) || is_link($payload)) stop_recovery('Extract the complete luxury update ZIP into public_html first.');
$script = file_get_contents($payload);
if (!is_string($script) || hash('sha256',$script) !== 'PAYLOAD_SHA256') stop_recovery('The selector file did not match the reviewed release. Extract the complete ZIP again.');
foreach (array('luxury-art.png'=>'ART_SHA256','architecture.png'=>'ARCH_SHA256') as $name=>$digest) {
 $image=$root.'/.np-luxury-files/'.$name;
 if (!is_file($image) || is_link($image) || hash_file('sha256',$image)!==$digest) stop_recovery('The local artwork did not match this release. Extract the complete luxury ZIP again.');
}
if (empty($_SESSION['np_luxury_nonce'])) $_SESSION['np_luxury_nonce']=bin2hex(random_bytes(24));
if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
 echo '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Install the NairaPips luxury design</title><main style="max-width:650px;margin:60px auto;font:18px/1.6 system-ui;padding:24px"><h1>Install the approved luxury design</h1><p>Install the approved gold waves, artwork and account selector directly into this homepage. A backup is made first. The design will load locally without waiting for the hosted styling script.</p><p>Source: '.htmlspecialchars(basename(dirname($source)).'/'.basename($source),ENT_QUOTES,'UTF-8').'</p><p>Admin pages, trader pages, prices and purchase switches stay unchanged.</p><form method="post"><input type="hidden" name="nonce" value="'.htmlspecialchars($_SESSION['np_luxury_nonce'],ENT_QUOTES,'UTF-8').'"><input type="hidden" name="source_hash" value="'.hash('sha256',$home).'"><input type="hidden" name="current_hash" value="'.hash('sha256',$current).'"><button style="padding:16px;font:inherit">Install luxury design</button></form></main>';
 exit;
}
foreach (array('nonce','source_hash','current_hash') as $field) if (!isset($_POST[$field]) || !is_string($_POST[$field])) stop_recovery('Reload this recovery page first.');
if (!hash_equals($_SESSION['np_luxury_nonce'],$_POST['nonce']) || !hash_equals(hash('sha256',$home),$_POST['source_hash']) || !hash_equals(hash('sha256',$current),$_POST['current_hash'])) stop_recovery('The homepage or backup changed. Reload before trying again.');
// Replace earlier injected bundles as one block; preserve unrelated page scripts and forms.
$home = preg_replace('~<!-- NP_(?:LIVE_CATALOGUE_V1|LUXURY_LOCAL_V3) -->\s*<script\b[^>]*>.*?</script>~is','',$home);
$home = preg_replace('~<script\b[^>]*src=["\']https://nairapips-api\.onrender\.com/public/landing-catalogue\.js[^"\']*["\'][^>]*>\s*</script>~i','',$home);
// Local reviewed fallback preserves the design if the hosted script is delayed.
$script = str_ireplace('</script','<\/script',$script);
$tag = '<!-- NP_LUXURY_LOCAL_V3 --><script>'.$script.'</script><script async src="https://nairapips-api.onrender.com/public/landing-catalogue.js?v=luxury-local-v3"></script>';
$home = preg_replace('~<title>.*?</title>~is','<title>NairaPips | Trade with distinction</title>',$home,1);
$updated = preg_replace_callback('~</body>~i',function($match) use ($tag) { return $tag.'</body>'; },$home,1);
if (!valid_home($updated)) stop_recovery('The recovered page could not be validated. No files were changed.');
$backup = dirname($root).'/np-homepage-backups/'.gmdate('Ymd-His').'-'.bin2hex(random_bytes(4));
if (!@mkdir($backup,0700,true) || !@copy($target,$backup.'/index-before.html')) stop_recovery('The current homepage could not be backed up. No files were changed.');
$temp = tempnam($root,'nphome');
if (!$temp || file_put_contents($temp,$updated,LOCK_EX)!==strlen($updated)) stop_recovery('The restored homepage could not be staged.');
@chmod($temp,0644);
if (!@rename($temp,$target)) { @unlink($temp); stop_recovery('The restored homepage could not be installed.'); }
unset($_SESSION['np_luxury_nonce']);
@unlink(__FILE__);
echo '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Luxury design installed</title><main style="max-width:650px;margin:60px auto;font:18px/1.6 system-ui;padding:24px"><h1>Luxury design installed</h1><p>The gold artwork and new design are installed locally. Admin, trader pages and business settings were not changed.</p><p><a href="/?restored='.gmdate('YmdHis').'">Open the updated homepage</a></p></main>';
