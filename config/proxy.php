<?php
// proxy.php — generic ccxt relay, InfinityFree-compatible.

// --- Polyfill for getallheaders() (unavailable under CGI/FPM) ---
if (!function_exists('getallheaders')) {
    function getallheaders() {
        $headers = [];
        foreach ($_SERVER as $name => $value) {
            if (substr($name, 0, 5) === 'HTTP_') {
                $key = str_replace(' ', '-',
                    ucwords(strtolower(str_replace('_', ' ', substr($name, 5)))));
                $headers[$key] = $value;
            }
        }
        return $headers;
    }
}

error_reporting(E_ALL);
ini_set('display_errors', '1');

$method = $_SERVER['REQUEST_METHOD'];

// --- Parse path: /proxy.php/<exchange>/<path> ---
$path   = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$script = $_SERVER['SCRIPT_NAME'] ?? '/proxy.php';
$rel    = ltrim(str_replace($script, '', $path), '/');
$parts  = explode('/', $rel, 2);
$exchange     = strtolower($parts[0] ?? '');
$exchangePath = '/' . ltrim($parts[1] ?? '', '/');

// --- Determine target host ---
$targetHost = $_SERVER['HTTP_X_PROXY_TARGET_HOST'] ?? '';
$known = [
    'bybit'  => 'api.bybit.com',
    'kucoin' => 'api.kucoin.com',
    'bitget' => 'api.bitget.com',
    'mexc'   => 'api.mexc.com',
];
if ($targetHost === '' && isset($known[$exchange])) {
    $targetHost = $known[$exchange];
}
if ($targetHost === '') {
    http_response_code(400);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'No target host']);
    exit;
}

// --- Build upstream URL ---
$query = $_SERVER['QUERY_STRING'] ?? '';
$url = 'https://' . $targetHost . $exchangePath;
if ($query) $url .= '?' . $query;

// --- Forward selected headers ---
$headers = [];
foreach (getallheaders() as $k => $v) {
    $lk = strtolower($k);
    if (in_array($lk, [
        'accept', 'authorization', 'user-agent', 'content-type',
        'x-bapi-api-key', 'x-bapi-sign', 'x-bapi-timestamp',
        'kc-api-key', 'kc-api-sign', 'kc-api-timestamp',
        'kc-api-passphrase', 'kc-api-key-version',
    ])) {
        $headers[] = "$k: $v";
    }
}
$headers[] = 'Expect:';

// --- Read body for write methods ---
$body = null;
if (in_array($method, ['POST', 'PUT', 'PATCH', 'DELETE'])) {
    $body = file_get_contents('php://input');
}

// --- Fire request with plain cURL ---
$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
curl_setopt($ch, CURLOPT_TIMEOUT, 20);
curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, true);
curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);

if ($body !== null) {
    curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
}

$response = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$err      = curl_error($ch);
curl_close($ch);

if ($err) {
    http_response_code(502);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'cURL: ' . $err]);
    exit;
}

http_response_code($httpCode);
header('Content-Type: application/json');
echo $response;