"""Tests for skills extraction — regression tests for the fix."""
import pytest
from pipeline.parsers import extract_skills


# ═════════════════════════════════════════════════════════════════════════════
# Basic extraction
# ═════════════════════════════════════════════════════════════════════════════

class TestSkillsBasic:
    def test_python_detected(self):
        skills, has_py, _ = extract_skills("Requires Python 3.x experience")
        assert "Python" in skills
        assert has_py == 1

    def test_sql_detected(self):
        skills, _, has_sql = extract_skills("Must know SQL and database design")
        assert "SQL" in skills
        assert has_sql == 1

    def test_python_and_sql(self):
        skills, has_py, has_sql = extract_skills("Python, SQL required")
        assert "Python" in skills
        assert "SQL" in skills
        assert has_py == 1
        assert has_sql == 1

    def test_empty_string(self):
        skills, has_py, has_sql = extract_skills("")
        assert skills == ""
        assert has_py == 0
        assert has_sql == 0

    def test_no_skills_in_text(self):
        skills, has_py, has_sql = extract_skills("We need a team player")
        assert skills == ""
        assert has_py == 0
        assert has_sql == 0


# ═════════════════════════════════════════════════════════════════════════════
# R language detection
# ═════════════════════════════════════════════════════════════════════════════

class TestSkillsRLanguage:
    def test_r_alone_is_noise(self):
        """R by itself without context is filtered out."""
        skills, _, _ = extract_skills("We need someone with good R skills")
        # R alone without statistical context should be dropped
        assert skills == "" or "R" not in skills.split(", ")

    def test_r_with_python_kept(self):
        """R alongside Python/SQL is kept."""
        skills, _, _ = extract_skills("Must know Python, R, and SQL")
        assert "R" in skills
        assert "Python" in skills
        assert "SQL" in skills

    def test_r_with_statistical_context_kept(self):
        """R alone but with statistical context is kept."""
        skills, _, _ = extract_skills("R statistical programming required")
        assert "R" in skills

    def test_r_with_rstudio_context_kept(self):
        """R alone but mentioning RStudio is kept."""
        skills, _, _ = extract_skills("Experience with R and RStudio")
        assert "R" in skills

    def test_r_case_sensitive(self):
        """Only uppercase R should match, not lowercase r."""
        skills1, _, _ = extract_skills("We use R for analysis")
        skills2, _, _ = extract_skills("we are looking for candidates")
        # Both should handle correctly (2nd has no R)
        assert "R" not in skills2

    def test_r_with_tidyverse_context_kept(self):
        """R alone but mentioning tidyverse is kept."""
        skills, _, _ = extract_skills("R programming with tidyverse packages")
        assert "R" in skills


# ═════════════════════════════════════════════════════════════════════════════
# Cloud & infrastructure skills
# ═════════════════════════════════════════════════════════════════════════════

class TestSkillsCloud:
    def test_aws(self):
        skills, _, _ = extract_skills("Experience with AWS services")
        assert "AWS" in skills

    def test_gcp(self):
        skills, _, _ = extract_skills("GCP (BigQuery, Dataflow)")
        assert "GCP" in skills
        assert "BigQuery" in skills

    def test_azure(self):
        skills, _, _ = extract_skills("Azure ML Studio experience")
        assert "Azure" in skills

    def test_docker(self):
        skills, _, _ = extract_skills("Docker and Kubernetes deployment")
        assert "Docker" in skills
        assert "Kubernetes" in skills

    def test_snowflake(self):
        skills, _, _ = extract_skills("Snowflake data warehouse")
        assert "Snowflake" in skills

    def test_databricks(self):
        skills, _, _ = extract_skills("Databricks and Spark experience")
        assert "Databricks" in skills
        assert "Spark" in skills


# ═════════════════════════════════════════════════════════════════════════════
# AI/ML skills
# ═════════════════════════════════════════════════════════════════════════════

class TestSkillsAiMl:
    def test_tensorflow(self):
        skills, _, _ = extract_skills("TensorFlow 2.x experience required")
        assert "TensorFlow" in skills

    def test_pytorch(self):
        skills, _, _ = extract_skills("PyTorch for deep learning models")
        assert "PyTorch" in skills

    def test_scikit_learn(self):
        skills, _, _ = extract_skills("scikit-learn, pandas, numpy")
        assert "Scikit-learn" in skills
        assert "Pandas" in skills
        assert "NumPy" in skills

    def test_langchain(self):
        skills, _, _ = extract_skills("LangChain for RAG pipelines")
        assert "LangChain" in skills
        assert "RAG" in skills

    def test_openai(self):
        skills, _, _ = extract_skills("OpenAI API integration")
        assert "OpenAI API" in skills

    def test_llm(self):
        skills, _, _ = extract_skills("Building LLM-powered applications")
        assert "LLM" in skills

    def test_nlp(self):
        skills, _, _ = extract_skills("NLP and text classification")
        assert "NLP" in skills

    def test_hugging_face(self):
        skills, _, _ = extract_skills("Hugging Face transformers library")
        assert "Hugging Face" in skills


# ═════════════════════════════════════════════════════════════════════════════
# Analytics skills
# ═════════════════════════════════════════════════════════════════════════════

class TestSkillsAnalytics:
    def test_tableau(self):
        skills, _, _ = extract_skills("Tableau dashboards and visualization")
        assert "Tableau" in skills

    def test_power_bi(self):
        skills, _, _ = extract_skills("Power BI reports for stakeholders")
        assert "Power BI" in skills

    def test_excel(self):
        skills, _, _ = extract_skills("Advanced Excel and pivot tables")
        assert "Excel" in skills

    def test_looker(self):
        skills, _, _ = extract_skills("Looker for business intelligence")
        assert "Looker" in skills

    def test_amplitude(self):
        skills, _, _ = extract_skills("Amplitude and Mixpanel for product analytics")
        assert "Amplitude" in skills
        assert "Mixpanel" in skills

    def test_ab_testing(self):
        skills, _, _ = extract_skills("A/B testing and experimentation")
        assert "A/B Testing" in skills

    def test_causal_inference(self):
        skills, _, _ = extract_skills("Causal inference methods required")
        assert "Causal Inference" in skills

    def test_bayesian(self):
        skills, _, _ = extract_skills("Bayesian statistics and modeling")
        assert "Bayesian" in skills


# ═════════════════════════════════════════════════════════════════════════════
# Full-text extraction (simulating body_raw join)
# ═════════════════════════════════════════════════════════════════════════════

class TestSkillsFullText:
    def test_long_description_extracts_multiple(self):
        """Simulate a full job description with many skills."""
        text = """
        Senior Data Scientist - AI Team

        Requirements:
        - 5+ years of Python programming
        - Strong SQL skills for data extraction
        - Experience with TensorFlow or PyTorch
        - Cloud experience (AWS or GCP preferred)
        - Familiarity with Snowflake or BigQuery
        - Experience with A/B testing and causal inference
        - Knowledge of LLM and NLP techniques
        """
        skills, has_py, has_sql = extract_skills(text)
        skill_list = [s.strip() for s in skills.split(",")]
        assert has_py == 1
        assert has_sql == 1
        assert "TensorFlow" in skills
        assert "PyTorch" in skills
        assert "AWS" in skills
        assert "LLM" in skills
        assert "NLP" in skills
        assert len(skill_list) >= 8

    def test_title_provides_skills(self):
        """Skills in title should be detected."""
        text = "Python Data Analyst - We need an analyst"
        skills, has_py, _ = extract_skills(text)
        assert has_py == 1
