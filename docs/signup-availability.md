# Signup email capacity

On 8 November the production HTTP health report showed all three ordinary
request slots occupied by `/send_email_otp` requests for more than 2,000 seconds.
The catalogue consequently returned 503. One stack was waiting on connection
resolution; two were waiting on the shared database SDK transport.

OTP storage now uses a separate REST request with explicit connect/read timeouts.
The code, expiry, verification requirements and configured mail sender remain
unchanged. The handler executes under a copied Flask request context with one
in-flight job per process and no waiting job queue. Its HTTP caller waits at most
12 seconds. The exclusive job permit is retained until the underlying operation
finishes, so a timeout followed by repeated requests cannot accumulate sends.

A timeout cannot cancel an external operation already in flight. The response
asks the customer to wait before requesting another code; it does not claim that
no email can arrive. A permanently unavailable provider may still need separate
recovery, but it cannot hold all catalogue slots through this route.

Tests use mock storage and sending only; deployment checks do not send real mail.
No offer terms, purchase switches, customer accounts or VPS programs are changed.
