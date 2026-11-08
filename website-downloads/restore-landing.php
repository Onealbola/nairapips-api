<?php
// One-time homepage recovery. No database, credentials, or trading changes.
header('Cache-Control: no-store');
header('X-Content-Type-Options: nosniff');
header('X-Robots-Tag: noindex, nofollow');
session_name('np_landing_recovery');
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
if (!valid_home($home)) {
 $candidates = glob(dirname($root).'/np-catalogue-backups/*/landing-original.html');
 if (!$candidates) stop_recovery('No original homepage backup was found. Keep this page open and ask for help locating your homepage backup.');
 rsort($candidates,SORT_STRING);
 $home = false;
 foreach ($candidates as $candidate) {
  if (is_link($candidate) || filesize($candidate)>5000000) continue;
  $body = file_get_contents($candidate);
  if (valid_home($body)) { $source=$candidate; $home=$body; break; }
 }
 if ($home === false) stop_recovery('The saved backups did not contain a valid homepage. No files were changed.');
}
$payload = $root.'/.np-landing-files/landing-catalogue.js';
if (!is_file($payload)) stop_recovery('Extract the complete recovery ZIP into public_html first.');
$script = file_get_contents($payload);
if (!is_string($script) || hash('sha256',$script) !== 'PAYLOAD_SHA256') stop_recovery('The selector file did not match the reviewed release. Extract the complete ZIP again.');
if (empty($_SESSION['np_landing_nonce'])) $_SESSION['np_landing_nonce']=bin2hex(random_bytes(24));
if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
 echo '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Restore NairaPips homepage</title><main style="max-width:650px;margin:60px auto;font:18px/1.6 system-ui;padding:24px"><h1>Restore the landing page</h1><p>Use the latest valid homepage backup and reconnect the modern account selector. The current homepage will be backed up first.</p><p>Source: '.htmlspecialchars(basename(dirname($source)).'/'.basename($source),ENT_QUOTES,'UTF-8').'</p><p>Admin pages, trader pages, prices and purchase switches stay unchanged.</p><form method="post"><input type="hidden" name="nonce" value="'.htmlspecialchars($_SESSION['np_landing_nonce'],ENT_QUOTES,'UTF-8').'"><input type="hidden" name="source_hash" value="'.hash('sha256',$home).'"><input type="hidden" name="current_hash" value="'.hash('sha256',$current).'"><button style="padding:16px;font:inherit">Restore homepage and new selector</button></form></main>';
 exit;
}
foreach (array('nonce','source_hash','current_hash') as $field) if (!isset($_POST[$field]) || !is_string($_POST[$field])) stop_recovery('Reload this recovery page first.');
if (!hash_equals($_SESSION['np_landing_nonce'],$_POST['nonce']) || !hash_equals(hash('sha256',$home),$_POST['source_hash']) || !hash_equals(hash('sha256',$current),$_POST['current_hash'])) stop_recovery('The homepage or backup changed. Reload before trying again.');
$home = preg_replace('~<!-- NP_LIVE_CATALOGUE_V1 -->~','',$home);
$home = preg_replace('~<script\b[^>]*src=["\']https://nairapips-api\.onrender\.com/public/landing-catalogue\.js[^"\']*["\'][^>]*>\s*</script>~i','',$home);
// Local reviewed fallback preserves the design if the hosted script is delayed.
$script = str_ireplace('</script','<\/script',$script);
$tag = '<!-- NP_LIVE_CATALOGUE_V1 --><script>'.$script.'</script><script src="https://nairapips-api.onrender.com/public/landing-catalogue.js?v=landing-recovery-20261108"></script>';
$updated = preg_replace_callback('~</body>~i',function($match) use ($tag) { return $tag.'</body>'; },$home,1);
if (!valid_home($updated)) stop_recovery('The recovered page could not be validated. No files were changed.');
$backup = dirname($root).'/np-homepage-backups/'.gmdate('Ymd-His').'-'.bin2hex(random_bytes(4));
if (!@mkdir($backup,0700,true) || !@copy($target,$backup.'/index-before.html')) stop_recovery('The current homepage could not be backed up. No files were changed.');
$temp = tempnam($root,'nphome');
if (!$temp || file_put_contents($temp,$updated,LOCK_EX)!==strlen($updated)) stop_recovery('The restored homepage could not be staged.');
@chmod($temp,0644);
if (!@rename($temp,$target)) { @unlink($temp); stop_recovery('The restored homepage could not be installed.'); }
unset($_SESSION['np_landing_nonce']);
@unlink(__FILE__);
echo '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Homepage restored</title><main style="max-width:650px;margin:60px auto;font:18px/1.6 system-ui;padding:24px"><h1>Homepage restored</h1><p>The new account selector is connected. Admin, trader pages and business settings were not changed.</p><p><a href="/?restored='.gmdate('YmdHis').'">Open landing page</a></p></main>';
