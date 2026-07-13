use std::io::{self, Write};
use std::time::Duration;

use grammers_client::{Client, SignInError};
use log::warn;

use super::shutdown::flood_wait_secs;

const MAX_AUTH_FLOOD_RETRIES: u32 = 3;

pub(super) async fn authorize_interactive(client: &Client, api_hash: &str) -> anyhow::Result<()> {
    print!("Enter phone number (international format): ");
    io::stdout().flush().ok();
    let mut phone = String::new();
    io::stdin().read_line(&mut phone)?;
    let phone = phone.trim();

    // request_login_code can hit FLOOD_WAIT if codes have been requested too
    // often; sleep the required time and retry instead of bailing out.
    let token = {
        let mut flood_retries = 0u32;
        loop {
            match client.request_login_code(phone, api_hash).await {
                Ok(token) => break token,
                Err(e) => {
                    let err_str = e.to_string();
                    if let Some(wait_secs) = flood_wait_secs(&err_str) {
                        if flood_retries >= MAX_AUTH_FLOOD_RETRIES {
                            return Err(anyhow::anyhow!(
                                "auth.sendCode still rate-limited after \
                                 {MAX_AUTH_FLOOD_RETRIES} waits: {err_str}"
                            ));
                        }
                        flood_retries += 1;
                        warn!(
                            "auth: FLOOD_WAIT - sleeping {wait_secs}s before retrying \
                             ({flood_retries}/{MAX_AUTH_FLOOD_RETRIES})"
                        );
                        tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }
    };

    print!("Enter the verification code sent via Telegram: ");
    io::stdout().flush().ok();
    let mut code = String::new();
    io::stdin().read_line(&mut code)?;
    let code = code.trim();

    match client.sign_in(&token, code).await {
        Ok(_) => Ok(()),
        Err(SignInError::PasswordRequired(pt)) => {
            let hint = pt.hint().unwrap_or_default();
            print!("2FA password (hint: {hint}): ");
            io::stdout().flush().ok();
            let mut pass = String::new();
            io::stdin().read_line(&mut pass)?;
            client.check_password(pt, pass.trim()).await?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
