import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


CRAWL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CRAWL_DIR))

from chat_prompts import extract_json_object  # noqa: E402
from chat_service import NewsChatService, _fallback_intent  # noqa: E402
from retrieval_service import normalize_query_tokens, score_article_for_query  # noqa: E402


class NewsChatbotServiceTests(unittest.TestCase):
    def test_extract_json_object_handles_code_fence(self):
        payload = extract_json_object(
            """```json
            {"intent":"latest_news","days":3}
            ```"""
        )
        self.assertEqual(payload["intent"], "latest_news")
        self.assertEqual(payload["days"], 3)

    def test_query_tokenization_prefers_meaningful_terms(self):
        tokens = normalize_query_tokens("今天 OpenAI 和 Claude 的最新动态")
        self.assertIn("openai", tokens)
        self.assertIn("claude", tokens)
        self.assertIn("最新动态", tokens)

    def test_score_article_for_query_boosts_title_matches(self):
        article = {
            "title": "OpenAI releases new Agent SDK",
            "summary": "A broad overview of agent tooling.",
            "content": "OpenAI shipped a new SDK for agents.",
            "total_score": 88,
            "spread_heat": 70,
            "published_at": "2026-07-22T08:00:00+00:00",
        }
        scores = score_article_for_query(article, ["openai", "agent"])
        self.assertGreater(scores["score"], 0.5)
        self.assertIn("openai", scores["matchedKeywords"])

    def test_fallback_intent_detects_latest_news(self):
        intent = _fallback_intent("给我看今天最新的新闻", [])
        self.assertEqual(intent["intent"], "latest_news")
        self.assertEqual(intent["days"], 3)
        self.assertEqual(intent["query"], "")

    def test_preview_search_returns_citations_without_saving_session(self):
        service = NewsChatService()
        fake_retrieval = {
            "query": "openai agent",
            "retrievalMode": "hybrid_sql_keyword_score_recency",
            "fallbackUsed": False,
            "sourceKeys": ["openai_blog"],
            "days": 7,
            "tokens": ["openai", "agent"],
            "matches": [
                {
                    "rank": 1,
                    "id": 1,
                    "title": "OpenAI releases new Agent SDK",
                    "url": "https://example.com/a",
                    "source": "OpenAI Blog",
                    "publishedAt": "2026-07-22T08:00:00+00:00",
                    "snippet": "OpenAI shipped a new SDK for agents.",
                    "score": 0.91,
                }
            ],
        }
        with patch("chat_service.retrieve_news_articles", new=AsyncMock(return_value=fake_retrieval)):
            result = asyncio.run(service.preview_search(query="openai agent", limit=5))

        self.assertEqual(result["citations"][0]["title"], "OpenAI releases new Agent SDK")
        self.assertEqual(result["retrieval"]["sourceKeys"], ["openai_blog"])

    def test_list_sessions_returns_compact_metadata(self):
        service = NewsChatService()
        with patch(
            "chat_service.list_news_chat_sessions",
            new=AsyncMock(
                return_value=[
                    {
                        "id": "s1",
                        "title": "OpenAI",
                        "data_json": [
                            {"role": "user", "content": "OpenAI 最近有什么动态？"},
                            {"role": "assistant", "content": "回答内容 [1]"},
                        ],
                        "created_at": "2026-07-23 10:00:00",
                        "updated_at": "2026-07-23 10:01:00",
                    }
                ]
            ),
        ):
            result = asyncio.run(service.list_sessions())

        self.assertEqual(result[0]["sessionId"], "s1")
        self.assertEqual(result[0]["title"], "OpenAI")
        self.assertEqual(result[0]["historyCount"], 2)
        self.assertEqual(result[0]["lastMessage"], "回答内容 [1]")

    def test_handle_message_persists_history_and_returns_answer(self):
        service = NewsChatService()
        with patch("chat_service.get_news_chat_session", new=AsyncMock(return_value=None)), \
            patch("chat_service.upsert_news_chat_session", new=AsyncMock(return_value={"id": "s1"})), \
            patch("chat_service._get_chat_client", return_value=None), \
            patch(
                "chat_service._route_intent",
                new=AsyncMock(
                    return_value={
                        "intent": "news_qa",
                        "query": "openai agent",
                        "source_keys": ["openai_blog"],
                        "days": 7,
                        "need_clarification": False,
                        "clarification_question": "",
                        "answer_style": "concise",
                        "confidence": 0.9,
                        "router": "fallback",
                    }
                ),
            ), \
            patch(
                "chat_service.retrieve_news_articles",
                new=AsyncMock(
                    return_value={
                        "query": "openai agent",
                        "retrievalMode": "hybrid_sql_keyword_score_recency",
                        "fallbackUsed": False,
                        "sourceKeys": ["openai_blog"],
                        "days": 7,
                        "tokens": ["openai", "agent"],
                        "matches": [
                            {
                                "rank": 1,
                                "id": 1,
                                "title": "OpenAI releases new Agent SDK",
                                "url": "https://example.com/a",
                                "source": "OpenAI Blog",
                                "publishedAt": "2026-07-22T08:00:00+00:00",
                                "snippet": "OpenAI shipped a new SDK for agents.",
                                "score": 0.91,
                            }
                        ],
                    }
                ),
            ), \
            patch("chat_service._generate_answer", new=AsyncMock(return_value="回答内容 [1]")):
            result = asyncio.run(service.handle_message(message="OpenAI Agent SDK 是什么？", session_id="s1"))

        self.assertEqual(result["sessionId"], "s1")
        self.assertEqual(result["answer"], "回答内容 [1]")
        self.assertEqual(result["citations"][0]["index"], 1)
        self.assertTrue(result["history"])


if __name__ == "__main__":
    unittest.main()
