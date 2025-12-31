// popup.js

// --- 1. ポップアップが開かれた時に、保存された設定を読み込む ---
document.addEventListener('DOMContentLoaded', () => {
  // ストレージから 'scrollAmount' を取得
  chrome.storage.local.get(['scrollAmount'], (result) => {
    if (result.scrollAmount) {
      // 保存された値があれば入力欄にセット
      document.getElementById('scrollAmount').value = result.scrollAmount;
    }
  });

  // (オプション) 現在実行中かどうかを確認してステータス表示を更新する機能を入れるならここですが、
  // 今回はまず「数値が戻る問題」の解決に集中します。
});

document.getElementById('startBtn').addEventListener('click', () => {
  const amountInput = document.getElementById('scrollAmount').value;
  const amount = parseInt(amountInput, 10);
  const finalAmount = isNaN(amount) ? 500 : amount;

  // --- 2. STARTボタンを押した時に、設定値を保存する ---
  chrome.storage.local.set({ scrollAmount: finalAmount }, () => {
    console.log('Settings saved:', finalAmount);
  });

  sendMessageToContent({ 
    command: "start", 
    scrollAmount: finalAmount 
  });
  
  document.getElementById('statusText').innerText = `Running (Scroll: ${finalAmount}px)...`;
});

document.getElementById('stopBtn').addEventListener('click', () => {
  sendMessageToContent({ command: "stop" });
  document.getElementById('statusText').innerText = "Status: Stopped";
});

function sendMessageToContent(message) {
  chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
    if (tabs[0]?.id) {
      chrome.tabs.sendMessage(tabs[0].id, message).catch(err => {
        console.error("Connection failed", err);
        document.getElementById('statusText').innerText = "Error: Reload Page required";
      });
    }
  });
}