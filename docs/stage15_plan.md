# Stage 15 implementation plan

1. Establish the repository layout and local quality gate.
2. Validate the Bundle for `dev` and `prod` targets.
3. Configure protected Azure DevOps variables for OAuth M2M.
4. Run CI on a feature branch and pull request.
5. Deploy the isolated smoke-test Job to dev.
6. Generate and review the existing Lakeflow Job Bundle definition.
7. Bind the existing dev Job instead of creating a duplicate.
8. Add production approval, deployment, verification, and rollback controls.
