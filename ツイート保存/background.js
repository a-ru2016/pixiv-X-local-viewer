// background.js

// 既に持っているファイル名のキャッシュ (メモリ上で高速判定するため)
// Setを使うことで、数万件あっても検索コストはほぼゼロ(O(1))です。
const downloadedFiles = new Set();
let isHistoryLoaded = false;

// --- 1. 起動時に過去の履歴を読み込む (負荷軽減のキモ) ---
// limit: 0 にすることで、直近だけでなく「全履歴」を取得します。
// これにより以前保存したファイルに対する (1) の発生を完全に防ぎます。
chrome.downloads.search({ limit: 0, state: 'complete' }, (results) => {
  results.forEach((item) => {
    // 存在し、かつ filename があるもの
    if (item.exists && item.filename) {
      // Windows(\) と Mac/Linux(/) のパス区切りを統一して処理
      const normalizedPath = item.filename.replace(/\\/g, '/');
      
      // "twitter_save" フォルダ内のファイルだけをキャッシュ対象にする
      // これにより無関係なダウンロード履歴によるメモリ圧迫を防ぎます
      if (normalizedPath.includes('twitter_save/')) {
        // パスから「ファイル名部分」だけを取り出す (ex: "twitter_save/abc.jpg" -> "abc.jpg")
        const fileName = normalizedPath.split('/').pop();
        downloadedFiles.add(fileName);
      }
    }
  });
  isHistoryLoaded = true;
  console.log(`History loaded: ${downloadedFiles.size} files in twitter_save cache.`);
});

// --- 2. メッセージ受信 ---
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  // 履歴読み込み完了までは処理しない（重複を防ぐため）
  if (!isHistoryLoaded) return;

  if (request.type === "download_image") {
    checkAndDownload(request.url, request.filename);
  }

  if (request.type === "save_json") {
    const jsonString = JSON.stringify(request.data, null, 2);
    const dataUri = 'data:application/json;charset=utf-8,' + encodeURIComponent(jsonString);
    // JSONも同様にチェックを通すことで重複保存を防ぐ
    checkAndDownload(dataUri, request.filename, true);
  }
});

function checkAndDownload(url, filename, isJson = false) {
  // 保存しようとしているファイル名 (ex: "twitter_save/user_123.jpg") から
  // ファイル名部分だけ抽出 (ex: "user_123.jpg")
  const targetName = filename.split('/').pop();

  // ★ここが最大のポイント★
  // メモリ上のリストに名前があれば、chrome.downloads.download を実行しない。
  // これにより Twitter への通信も発生せず、(1) も生成されない。
  if (downloadedFiles.has(targetName)) {
    // コンソールログも減らして処理を軽くする
    // console.log(`Skipped (Exists): ${targetName}`); 
    return;
  }

  // ダウンロード実行
  chrome.downloads.download({
    url: url,
    filename: filename,
    // ここで uniquify を指定していても、上の has チェックで弾くので (1) は基本的に発生しない
    // 万が一の競合時は (1) になるが、エラーで止まるよりは良い
    conflictAction: "uniquify", 
    saveAs: false
  }, (downloadId) => {
    if (chrome.runtime.lastError) {
      console.warn(`Failed: ${filename}`, chrome.runtime.lastError.message);
    } else {
      // 成功したら即座にキャッシュに追加し、
      // ページスクロール中に同じ画像が何度も出てきても再ダウンロードしないようにする
      downloadedFiles.add(targetName);
    }
  });
}