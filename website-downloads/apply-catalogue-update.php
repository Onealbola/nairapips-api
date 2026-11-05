<?php
// One-time owner-run installer. No database credentials or financial actions.
header('Cache-Control: no-store');
header('X-Content-Type-Options: nosniff');
header('X-Robots-Tag: noindex, nofollow');
session_name('np_catalogue_install');
session_start();
if (empty($_SESSION['np_catalogue_nonce'])) $_SESSION['np_catalogue_nonce'] = bin2hex(random_bytes(24));
function fail_install($message) { http_response_code(400); echo '<h2>Update not applied</h2><p>'.htmlspecialchars($message, ENT_QUOTES, 'UTF-8').'</p>'; exit; }
if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
 echo '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>NairaPips website update</title><main style="max-width:650px;margin:60px auto;font:18px/1.6 system-ui;padding:24px"><h1>Connect your challenge catalogue</h1><p>This updates the landing-page catalogue and trader Plans / Purchase Challenge pages. Existing website files are backed up first. It does not enable sales or change purchases.</p><form method="post"><input type="hidden" name="nonce" value="'.htmlspecialchars($_SESSION['np_catalogue_nonce'],ENT_QUOTES,'UTF-8').'"><button style="padding:16px;font:inherit">Apply website update</button></form></main>'; exit;
}
if (!isset($_POST['nonce']) || !is_string($_POST['nonce']) || !hash_equals($_SESSION['np_catalogue_nonce'], $_POST['nonce'])) fail_install('Reload this installer and try again.');
$root = __DIR__;
$landing = is_file($root.'/index.html') ? $root.'/index.html' : $root.'/index.php';
$trader = $root.'/dashboard/trader_clean.html';
$payload = $root.'/.np-catalogue-files/trader_clean.html';
if (!is_file($landing) || !is_file($trader) || !is_file($payload)) fail_install('Extract the complete ZIP directly into public_html. The existing landing page and dashboard folder must both be present there.');
$home = file_get_contents($landing);
$dashboard = file_get_contents($payload);
if ($home === false || $dashboard === false || stripos($home,'</body>') === false || strpos($home,'npStandardPlansGrid') === false || strpos($home,'npPremiumPlansGrid') === false) fail_install('The existing landing page could not be safely read. No file was changed.');
if (hash('sha256',$dashboard) !== '64812343a3293892afa1c298a311fe62da1da2cec5bad70a4e754d9d9dccd1ec') fail_install('The trader-page payload could not be verified. Extract the complete ZIP again.');
$marker = '<!-- NP_LIVE_CATALOGUE_V1 -->';
$tag = $marker.'<script src="https://nairapips-api.onrender.com/public/landing-catalogue.js?v=20261105"></script>';
if (strpos($home,$marker) === false) $home = preg_replace('~</body>~i', $tag.'</body>', $home, 1);
$backup = dirname($root).'/np-catalogue-backups/'.gmdate('Ymd-His').'-'.bin2hex(random_bytes(4));
if (!@mkdir($backup,0700,true)) fail_install('A backup folder could not be created beside public_html. No file was changed.');
$targets = array($landing=>$home, $trader=>$dashboard);
$copies = array(); $staged = array();
foreach ($targets as $file=>$content) {
 $saved = $backup.'/'.(count($copies)===0?'landing-original':'trader-original').'.html';
 if (!@copy($file,$saved)) fail_install('An original file could not be backed up. No website file was changed.');
 $copies[$file]=$saved;
 $temp = @tempnam(dirname($file),'npcatalogue');
 if (!$temp || file_put_contents($temp,$content,LOCK_EX) !== strlen($content)) fail_install('Updated files could not be staged. No website file was changed.');
 @chmod($temp,fileperms($file)&0777); $staged[$file]=$temp;
}
foreach ($staged as $file=>$temp) {
 if (!@rename($temp,$file)) {
  foreach ($copies as $original=>$saved) @copy($saved,$original);
  fail_install('The update could not be completed. Original files were restored from backup.');
 }
}
unset($_SESSION['np_catalogue_nonce']);
@unlink(__FILE__);
echo '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Update complete</title><main style="max-width:650px;margin:60px auto;font:18px/1.6 system-ui;padding:24px"><h1>Website update complete</h1><p>Refresh the landing page and sign in to your trader dashboard. Plans and Purchase Challenge now use the same saved catalogue.</p><p>Original files are backed up in '.htmlspecialchars($backup,ENT_QUOTES,'UTF-8').'. Purchase switches remain under your control.</p><p><a href="/">Open landing page</a> · <a href="/dashboard/trader_clean.html">Open trader dashboard</a></p></main>';
